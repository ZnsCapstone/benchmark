#!/usr/bin/env bash
set -euo pipefail
exec /home/zns-team4/tools/f2fs-native-reserve-build/toolroot/usr/bin/autom4te \
  --prepend-include=/home/zns-team4/tools/f2fs-native-reserve-build/toolroot/usr/share/autoconf "$@"
