"""
Setup script for building Cython extensions.
Run: python setup_cython.py build_ext --inplace
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import os

# Only include extensions for files that exist
extensions = []

# Binance CS shared memory reader
if os.path.exists("binance_cs_shared_memory_reader_optimized.pyx"):
    extensions.append(Extension(
        "binance_cs_shared_memory_reader_optimized",
        ["binance_cs_shared_memory_reader_optimized.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=["-O3", "-ffast-math", "-march=native"],  # Aggressive optimizations + CPU-specific
        language="c"
    ))

# EXCHANGE shared memory reader
if os.path.exists("EXCHANGE_shared_memory_reader_optimized.pyx"):
    extensions.append(Extension(
        "EXCHANGE_shared_memory_reader_optimized",
        ["EXCHANGE_shared_memory_reader_optimized.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=["-O3", "-ffast-math", "-march=native"],  # Aggressive optimizations + CPU-specific
        language="c"
    ))

setup(
    name="BTCTURK WebSocket Optimizations",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",
            'boundscheck': False,
            'wraparound': False,
            'cdivision': True,
            'initializedcheck': False,
            'optimize.use_switch': True,
        },
        annotate=False  # Set to True to generate HTML annotation file for debugging
    ),
    zip_safe=False,
)

