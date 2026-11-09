#!/usr/bin/env bash
# Prepares a Linux host for the engine. Run as root; settings marked as
# needing a reboot take effect after the kernel command line is updated.
set -euo pipefail

HOT_CPUS="${HOT_CPUS:-2,3}"
HOUSEKEEPING_CPU="${HOUSEKEEPING_CPU:-0}"

echo "Setting the CPU governor to performance, where the host exposes one."
for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    [ -f "$governor" ] && echo performance > "$governor"
done

echo "Steering interrupts to CPU ${HOUSEKEEPING_CPU}."
mask=$(printf '%x' $((1 << HOUSEKEEPING_CPU)))
for affinity in /proc/irq/*/smp_affinity; do
    echo "$mask" > "$affinity" 2>/dev/null || true
done

if ! grep -q "isolcpus=" /proc/cmdline; then
    cat <<EOF

To isolate the hot CPUs from the scheduler, add this to GRUB_CMDLINE_LINUX in
/etc/default/grub, run update-grub and reboot:

    isolcpus=${HOT_CPUS} nohz_full=${HOT_CPUS} rcu_nocbs=${HOT_CPUS}
EOF
fi
