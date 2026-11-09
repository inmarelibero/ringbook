//! A flat price-indexed L2 order book.
//!
//! Each side is a preallocated array of quantities indexed by price, plus a
//! bitmap of occupied levels. The best level is cached; when it empties, the
//! bitmap finds the next occupied level without walking empty prices. No
//! operation allocates after construction.

#![forbid(unsafe_code)]

use ringbook_core::{Price, Qty, Side};

const WORD_BITS: usize = u64::BITS as usize;

/// Why a book could not be constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BookError {
    /// The tick must be positive.
    InvalidTick,
    /// The window must hold at least 64 levels.
    WindowTooSmall,
}

/// Counters describing a book's maintenance work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BookStats {
    /// How many times the window has moved.
    pub recentres: u64,
    /// Occupied levels discarded because a recentre moved past them.
    pub levels_dropped: u64,
    /// Updates ignored because their price was outside the window.
    pub out_of_window: u64,
    /// Updates ignored because their price was not a multiple of the tick.
    pub off_grid: u64,
}

#[derive(Debug)]
struct Ladder {
    quantities: Box<[i64]>,
    occupied: Box<[u64]>,
    count: usize,
}

impl Ladder {
    fn new(levels: usize) -> Self {
        Self {
            quantities: vec![0; levels].into_boxed_slice(),
            occupied: vec![0; levels.div_ceil(WORD_BITS)].into_boxed_slice(),
            count: 0,
        }
    }

    #[inline]
    fn set(&mut self, index: usize, qty: i64) {
        let word = index / WORD_BITS;
        let mask = 1_u64 << (index % WORD_BITS);
        let was_occupied = self.occupied[word] & mask != 0;
        self.quantities[index] = qty;
        if qty == 0 {
            self.occupied[word] &= !mask;
            self.count -= usize::from(was_occupied);
        } else {
            self.occupied[word] |= mask;
            self.count += usize::from(!was_occupied);
        }
    }

    /// Finds the highest occupied index at or below `index`.
    #[inline]
    fn highest_at_or_below(&self, index: usize) -> Option<usize> {
        let mut word = index / WORD_BITS;
        let shift = WORD_BITS - 1 - index % WORD_BITS;
        let mut bits = self.occupied[word] << shift >> shift;
        loop {
            if bits != 0 {
                return Some(word * WORD_BITS + WORD_BITS - 1 - bits.leading_zeros() as usize);
            }
            word = word.checked_sub(1)?;
            bits = self.occupied[word];
        }
    }

    /// Finds the lowest occupied index at or above `index`.
    #[inline]
    fn lowest_at_or_above(&self, index: usize) -> Option<usize> {
        let mut word = index / WORD_BITS;
        let mut bits = self.occupied[word] & (u64::MAX << (index % WORD_BITS));
        loop {
            if bits != 0 {
                return Some(word * WORD_BITS + bits.trailing_zeros() as usize);
            }
            word += 1;
            bits = *self.occupied.get(word)?;
        }
    }

    fn clear(&mut self) {
        for word in 0..self.occupied.len() {
            let mut bits = self.occupied[word];
            while bits != 0 {
                let index = word * WORD_BITS + bits.trailing_zeros() as usize;
                self.quantities[index] = 0;
                bits &= bits - 1;
            }
            self.occupied[word] = 0;
        }
        self.count = 0;
    }

    /// Moves the contents down by `shift` indices, or up if `shift` is
    /// negative, and returns how many occupied levels fell off the array.
    fn shift(&mut self, shift: isize) -> u64 {
        let len = self.quantities.len();
        let magnitude = shift.unsigned_abs().min(len);
        let lost_range = if shift > 0 {
            0..magnitude
        } else {
            len - magnitude..len
        };
        let lost = self.quantities[lost_range]
            .iter()
            .filter(|&&qty| qty != 0)
            .count() as u64;

        if shift > 0 {
            self.quantities.copy_within(magnitude.., 0);
            self.quantities[len - magnitude..].fill(0);
        } else {
            self.quantities.copy_within(..len - magnitude, magnitude);
            self.quantities[..magnitude].fill(0);
        }

        self.occupied.fill(0);
        self.count = 0;
        for index in 0..len {
            if self.quantities[index] != 0 {
                self.occupied[index / WORD_BITS] |= 1 << (index % WORD_BITS);
                self.count += 1;
            }
        }
        lost
    }
}

/// One instrument's L2 book at one venue.
#[derive(Debug)]
pub struct Book {
    tick: i64,
    levels: usize,
    /// The price of index zero, always a multiple of the tick.
    base: i64,
    bids: Ladder,
    asks: Ladder,
    best_bid: Option<usize>,
    best_ask: Option<usize>,
    stats: BookStats,
}

impl Book {
    /// Creates a book whose window holds `levels` prices, `tick` apart.
    ///
    /// # Errors
    ///
    /// Returns a [`BookError`] if `tick` isn't positive or `levels` is below 64.
    pub fn new(tick: i64, levels: usize) -> Result<Self, BookError> {
        if tick <= 0 {
            return Err(BookError::InvalidTick);
        }
        if levels < WORD_BITS {
            return Err(BookError::WindowTooSmall);
        }
        Ok(Self {
            tick,
            levels,
            base: 0,
            bids: Ladder::new(levels),
            asks: Ladder::new(levels),
            best_bid: None,
            best_ask: None,
            stats: BookStats::default(),
        })
    }

    /// Sets the absolute quantity at a price; zero removes the level.
    #[inline]
    pub fn apply(&mut self, side: Side, price: Price, qty: Qty) {
        if price.0 % self.tick != 0 {
            self.stats.off_grid += 1;
            return;
        }
        if self.is_empty() && !qty.is_zero() {
            self.base = self.centred_base(price.0);
        }

        let mut index = self.index_of(price);
        if index.is_none() && self.improves_best(side, price, qty) {
            // A jump past the window edge must not lose the new best level.
            self.recentre_on(price);
            index = self.index_of(price);
        }
        let Some(index) = index else {
            self.stats.out_of_window += 1;
            return;
        };

        match side {
            Side::Buy => {
                self.bids.set(index, qty.0);
                self.best_bid = Self::next_bid(&self.bids, self.best_bid, index, qty);
            }
            Side::Sell => {
                self.asks.set(index, qty.0);
                self.best_ask = Self::next_ask(&self.asks, self.best_ask, index, qty);
            }
        }
        self.recentre_if_near_edge();
    }

    #[inline]
    fn next_bid(bids: &Ladder, best: Option<usize>, index: usize, qty: Qty) -> Option<usize> {
        match best {
            _ if !qty.is_zero() => Some(best.map_or(index, |current| current.max(index))),
            Some(current) if current == index => bids.highest_at_or_below(index),
            other => other,
        }
    }

    #[inline]
    fn next_ask(asks: &Ladder, best: Option<usize>, index: usize, qty: Qty) -> Option<usize> {
        match best {
            _ if !qty.is_zero() => Some(best.map_or(index, |current| current.min(index))),
            Some(current) if current == index => asks.lowest_at_or_above(index),
            other => other,
        }
    }

    fn centred_base(&self, price: i64) -> i64 {
        price - self.tick * (self.levels / 2) as i64
    }

    #[inline]
    fn index_of(&self, price: Price) -> Option<usize> {
        usize::try_from((price.0 - self.base) / self.tick)
            .ok()
            .filter(|&index| index < self.levels)
    }

    #[inline]
    fn improves_best(&self, side: Side, price: Price, qty: Qty) -> bool {
        !qty.is_zero()
            && match side {
                Side::Buy => self.best_bid().is_none_or(|(best, _)| price > best),
                Side::Sell => self.best_ask().is_none_or(|(best, _)| price < best),
            }
    }

    #[inline]
    fn recentre_if_near_edge(&mut self) {
        let margin = self.levels / 8;
        let near_edge = |best: Option<usize>| {
            best.is_some_and(|index| index < margin || index >= self.levels - margin)
        };
        if near_edge(self.best_bid) || near_edge(self.best_ask) {
            self.recentre();
        }
    }

    #[cold]
    fn recentre(&mut self) {
        let Some(centre) = self
            .mid()
            .or(self.best_bid().map(|(price, _)| price))
            .or(self.best_ask().map(|(price, _)| price))
        else {
            return;
        };
        self.recentre_on(centre);
    }

    #[cold]
    fn recentre_on(&mut self, centre: Price) {
        let aligned = centre.0 - centre.0.rem_euclid(self.tick);
        let base = self.centred_base(aligned);
        let shift = (base - self.base) / self.tick;
        if shift == 0 {
            return;
        }
        let shift = shift as isize;

        self.stats.recentres += 1;
        self.stats.levels_dropped += self.bids.shift(shift) + self.asks.shift(shift);
        self.base = base;
        self.best_bid = self.bids.highest_at_or_below(self.levels - 1);
        self.best_ask = self.asks.lowest_at_or_above(0);
    }

    /// Removes every level, in preparation for a full snapshot.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.best_bid = None;
        self.best_ask = None;
    }

    /// Returns whether the book holds no levels.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.count == 0 && self.asks.count == 0
    }

    #[inline]
    fn price_at(&self, index: usize) -> Price {
        Price(self.base + self.tick * index as i64)
    }

    /// Returns the best bid.
    #[inline]
    #[must_use]
    pub fn best_bid(&self) -> Option<(Price, Qty)> {
        self.best_bid
            .map(|index| (self.price_at(index), Qty(self.bids.quantities[index])))
    }

    /// Returns the best ask.
    #[inline]
    #[must_use]
    pub fn best_ask(&self) -> Option<(Price, Qty)> {
        self.best_ask
            .map(|index| (self.price_at(index), Qty(self.asks.quantities[index])))
    }

    /// Returns the midpoint, rounded down, when both sides are present.
    #[inline]
    #[must_use]
    pub fn mid(&self) -> Option<Price> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        Some(Price(bid.0.midpoint(ask.0)))
    }

    /// Returns the quantity resting at a price; zero if none or out of window.
    #[inline]
    #[must_use]
    pub fn qty_at(&self, side: Side, price: Price) -> Qty {
        let ladder = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        if price.0 % self.tick != 0 {
            return Qty::ZERO;
        }
        usize::try_from((price.0 - self.base) / self.tick)
            .ok()
            .and_then(|index| ladder.quantities.get(index))
            .map_or(Qty::ZERO, |&qty| Qty(qty))
    }

    /// Iterates a side's levels from the best price outwards.
    #[must_use]
    pub fn levels(&self, side: Side) -> Levels<'_> {
        let next = match side {
            Side::Buy => self.best_bid,
            Side::Sell => self.best_ask,
        };
        Levels {
            book: self,
            side,
            next,
        }
    }

    /// Returns the book's maintenance counters.
    #[must_use]
    pub fn stats(&self) -> BookStats {
        self.stats
    }
}

/// An iterator over one side's levels. Created by [`Book::levels`].
#[derive(Debug)]
pub struct Levels<'book> {
    book: &'book Book,
    side: Side,
    next: Option<usize>,
}

impl Iterator for Levels<'_> {
    type Item = (Price, Qty);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next?;
        let (ladder, following) = match self.side {
            Side::Buy => (
                &self.book.bids,
                index
                    .checked_sub(1)
                    .and_then(|below| self.book.bids.highest_at_or_below(below)),
            ),
            Side::Sell => (
                &self.book.asks,
                (index + 1 < self.book.levels)
                    .then(|| self.book.asks.lowest_at_or_above(index + 1))
                    .flatten(),
            ),
        };
        self.next = following;
        Some((self.book.price_at(index), Qty(ladder.quantities[index])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn book() -> Book {
        Book::new(10, 1024).unwrap()
    }

    #[test]
    fn rejects_invalid_construction() {
        assert_eq!(Book::new(0, 1024).unwrap_err(), BookError::InvalidTick);
        assert_eq!(Book::new(1, 10).unwrap_err(), BookError::WindowTooSmall);
    }

    #[test]
    fn tracks_best_levels_and_mid() {
        let mut book = book();
        book.apply(Side::Buy, Price(1000), Qty(5));
        book.apply(Side::Buy, Price(990), Qty(7));
        book.apply(Side::Sell, Price(1020), Qty(3));
        assert_eq!(book.best_bid(), Some((Price(1000), Qty(5))));
        assert_eq!(book.best_ask(), Some((Price(1020), Qty(3))));
        assert_eq!(book.mid(), Some(Price(1010)));
    }

    #[test]
    fn finds_the_next_level_when_the_best_empties() {
        let mut book = book();
        book.apply(Side::Buy, Price(1000), Qty(5));
        book.apply(Side::Buy, Price(500), Qty(7));
        book.apply(Side::Sell, Price(2000), Qty(1));
        book.apply(Side::Sell, Price(3000), Qty(2));

        book.apply(Side::Buy, Price(1000), Qty::ZERO);
        book.apply(Side::Sell, Price(2000), Qty::ZERO);
        assert_eq!(book.best_bid(), Some((Price(500), Qty(7))));
        assert_eq!(book.best_ask(), Some((Price(3000), Qty(2))));
    }

    #[test]
    fn removing_an_absent_level_is_harmless() {
        let mut book = book();
        book.apply(Side::Buy, Price(1000), Qty(5));
        book.apply(Side::Buy, Price(900), Qty::ZERO);
        assert_eq!(book.best_bid(), Some((Price(1000), Qty(5))));
        assert_eq!(book.levels(Side::Buy).count(), 1);
    }

    #[test]
    fn clear_removes_everything_and_allows_reuse() {
        let mut book = book();
        book.apply(Side::Buy, Price(1000), Qty(5));
        book.apply(Side::Sell, Price(1010), Qty(5));
        book.clear();
        assert!(book.is_empty());
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.qty_at(Side::Buy, Price(1000)), Qty::ZERO);

        book.apply(Side::Buy, Price(900_000), Qty(1));
        assert_eq!(book.best_bid(), Some((Price(900_000), Qty(1))));
    }

    #[test]
    fn iterates_levels_from_the_best_outwards() {
        let mut book = book();
        for (price, qty) in [(1000, 1), (980, 2), (950, 3)] {
            book.apply(Side::Buy, Price(price), Qty(qty));
        }
        let levels: Vec<_> = book.levels(Side::Buy).map(|(p, q)| (p.0, q.0)).collect();
        assert_eq!(levels, [(1000, 1), (980, 2), (950, 3)]);
    }

    #[test]
    fn counts_rejected_updates() {
        let mut book = book();
        book.apply(Side::Buy, Price(1005), Qty(1));
        assert_eq!(book.stats().off_grid, 1);
        book.apply(Side::Buy, Price(100_000), Qty(1));
        book.apply(Side::Buy, Price(100_000 - 10 * 2048), Qty(1));
        assert_eq!(book.stats().out_of_window, 1);
    }

    #[test]
    fn recentres_as_the_market_drifts_and_keeps_nearby_levels() {
        let mut book = book();
        let mut price = 100_000;
        for _ in 0..4_000 {
            book.apply(Side::Buy, Price(price), Qty(1));
            book.apply(Side::Sell, Price(price), Qty::ZERO);
            book.apply(Side::Sell, Price(price + 10), Qty(1));
            price += 10;
        }
        assert!(book.stats().recentres > 0);
        assert_eq!(book.best_bid(), Some((Price(price - 10), Qty(1))));
        assert_eq!(book.qty_at(Side::Buy, Price(price - 100)), Qty(1));
        assert!(book.stats().levels_dropped > 0);
    }

    #[derive(Clone, Debug)]
    struct Update {
        side: Side,
        offset: i64,
        qty: i64,
    }

    fn update() -> impl Strategy<Value = Update> {
        (any::<bool>(), -100_i64..100, 0_i64..4).prop_map(|(buy, offset, qty)| Update {
            side: if buy { Side::Buy } else { Side::Sell },
            offset,
            qty,
        })
    }

    fn apply_to_model(model: &mut BTreeMap<i64, i64>, price: i64, qty: i64) {
        if qty == 0 {
            model.remove(&price);
        } else {
            model.insert(price, qty);
        }
    }

    proptest! {
        #[test]
        fn matches_a_reference_book(updates in proptest::collection::vec(update(), 0..400)) {
            let mut book = Book::new(10, 4096).unwrap();
            let mut bids = BTreeMap::new();
            let mut asks = BTreeMap::new();

            for update in updates {
                let price = 500_000 + update.offset * 10;
                book.apply(update.side, Price(price), Qty(update.qty));
                let model = if update.side == Side::Buy { &mut bids } else { &mut asks };
                apply_to_model(model, price, update.qty);

                prop_assert_eq!(book.best_bid().map(|(p, q)| (p.0, q.0)),
                    bids.iter().next_back().map(|(p, q)| (*p, *q)));
                prop_assert_eq!(book.best_ask().map(|(p, q)| (p.0, q.0)),
                    asks.iter().next().map(|(p, q)| (*p, *q)));
            }

            let book_bids: Vec<_> = book.levels(Side::Buy).map(|(p, q)| (p.0, q.0)).collect();
            let model_bids: Vec<_> = bids.iter().rev().map(|(p, q)| (*p, *q)).collect();
            prop_assert_eq!(book_bids, model_bids);
            let book_asks: Vec<_> = book.levels(Side::Sell).map(|(p, q)| (p.0, q.0)).collect();
            let model_asks: Vec<_> = asks.iter().map(|(p, q)| (*p, *q)).collect();
            prop_assert_eq!(book_asks, model_asks);
        }

        #[test]
        fn keeps_levels_near_the_market_across_recentres(
            steps in proptest::collection::vec((-40_i64..=40, 1_i64..4), 1..600),
        ) {
            // A small window forces recentres; levels near the mid must survive them.
            let mut book = Book::new(1, 256).unwrap();
            let mut mid = 1_000_000_i64;
            let mut bids = BTreeMap::new();
            let mut asks = BTreeMap::new();
            book.apply(Side::Buy, Price(mid - 1), Qty(1));
            book.apply(Side::Sell, Price(mid + 1), Qty(1));
            bids.insert(mid - 1, 1);
            asks.insert(mid + 1, 1);

            for (step, qty) in steps {
                mid += step;
                bids.retain(|price, _| *price < mid);
                asks.retain(|price, _| *price > mid);
                let stale: Vec<i64> = book.levels(Side::Buy).map(|(p, _)| p.0).filter(|p| *p >= mid).collect();
                for price in stale { book.apply(Side::Buy, Price(price), Qty::ZERO); }
                let stale: Vec<i64> = book.levels(Side::Sell).map(|(p, _)| p.0).filter(|p| *p <= mid).collect();
                for price in stale { book.apply(Side::Sell, Price(price), Qty::ZERO); }

                book.apply(Side::Buy, Price(mid - 1), Qty(qty));
                book.apply(Side::Sell, Price(mid + 1), Qty(qty));
                prop_assert_eq!(book.best_bid(), Some((Price(mid - 1), Qty(qty))));
                prop_assert_eq!(book.best_ask(), Some((Price(mid + 1), Qty(qty))));
            }
        }
    }
}
