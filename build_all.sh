#!/bin/bash

cd src
make -f Makefile.Linux.GCC64 clobber
make -f Makefile.Linux.GCC64D clobber
make -f Makefile.Linux.GCC64
make -f Makefile.Linux.GCC64D
make -f Makefile.Linux.GCC64 install_lib
make -f Makefile.Linux.GCC64D install_lib
make -f Makefile.Linux.GCC64 install_hdr

