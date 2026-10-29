#!/usr/bin/env bash
# team4 호스트 전용 격리 빌드. 사전에 apt-get download/dpkg-deb -x로
# autoconf/automake/libtool을 toolroot에 추출한다. 시스템 설치는 하지 않는다.
set -euo pipefail
BUILD=/home/zns-team4/tools/f2fs-native-reserve-build
TOOLS="$BUILD/toolroot/usr"
[[ $(id -un) == zns-team4 && -d $TOOLS/share/autoconf ]]
export PATH="$TOOLS/bin:$PATH"
export AC_MACRODIR="$TOOLS/share/autoconf"
export M4PATH="$TOOLS/share/autoconf"
export trailer_m4="$TOOLS/share/autoconf/autoconf/trailer.m4"
export autom4te_perllibdir="$TOOLS/share/autoconf"
export AUTOM4TE="$BUILD/autom4te-wrapper"
export AUTOCONF="$TOOLS/bin/autoconf"
export AUTOHEADER="$TOOLS/bin/autoheader"
export ACLOCAL="$TOOLS/bin/aclocal-1.16 --automake-acdir=$TOOLS/share/aclocal-1.16 -I $TOOLS/share/aclocal"
export AUTOMAKE="$TOOLS/bin/automake-1.16 --libdir=$TOOLS/share/automake-1.16"
export AUTOMAKE_PERLLIBDIR="$TOOLS/share/automake-1.16"
export LIBTOOLIZE="$TOOLS/bin/libtoolize"
export _lt_pkgdatadir="$TOOLS/share/libtool"
export PERL5LIB="$TOOLS/share/automake-1.16:$TOOLS/share/autoconf${PERL5LIB:+:$PERL5LIB}"
cd "$BUILD"
if [[ ! -e source ]]; then
  git clone --no-checkout https://git.kernel.org/pub/scm/linux/kernel/git/jaegeuk/f2fs-tools.git source
  git -C source checkout --detach 03aa49e4959fc986d87cfa3cef9a8ccd4aabdb74
fi
cd source
[[ $(git rev-parse HEAD) == 03aa49e4959fc986d87cfa3cef9a8ccd4aabdb74 ]]
"$TOOLS/bin/aclocal-1.16" --automake-acdir="$TOOLS/share/aclocal-1.16" --system-acdir="$TOOLS/share/aclocal"
"$AUTOHEADER"
"$AUTOCONF"
"$LIBTOOLIZE"
"$TOOLS/bin/automake-1.16" --libdir="$TOOLS/share/automake-1.16" -a -c
./configure --prefix="$BUILD/install" --disable-shared
make -j2
sha256sum mkfs/mkfs.f2fs fsck/fsck.f2fs
mkfs/mkfs.f2fs -V
