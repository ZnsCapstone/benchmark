#!/usr/bin/env python3
"""2번 장기 평가: DM 16조건 후 Native 8조건, 판정90%, 각 측정3600초."""
import importlib.util
from pathlib import Path

HERE=Path(__file__).resolve().parent;ROOT=HERE.parent

def load(name,file):
    spec=importlib.util.spec_from_file_location(name,HERE/file)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

def main():
    dm=load('long_dm','run-final-prefill-kafka-20261020.py')
    dm.LONG_RUN=True
    dm.REL='results/final/test2-long-gc-dm-20261020'
    dm.INPUT=ROOT/'results/final/long-gc-dm-input-20261020'
    dm.BUILD='/home/zns/test2-long-gc-dm-20261020'
    dm.main()
    native=load('long_native','run-native-kafka-test2-controller.py')
    native.LONG_RUN=True
    native.REL='results/final/test2-long-gc-native-20261020'
    native.INPUT=ROOT/'results/final/long-gc-native-input-20261020'
    native.BUILD='/home/zns/test2-long-gc-native-20261020'
    native.main()

if __name__=='__main__':main()
