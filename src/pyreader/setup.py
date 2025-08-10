#!/usr/bin/env python3
"""
Setup script for dftracer Python reader module.

This setup.py uses CMake to build everything including Cython compilation.
The actual build logic is in CMakeLists.txt.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext

class CMakeBuild(build_ext):
    """Custom build_ext that runs CMake."""
    
    def run(self):
        # Check for CMake
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build this package")
        
        # Check for Cython
        try:
            import Cython
            print(f"Found Cython version: {Cython.__version__}")
        except ImportError:
            raise RuntimeError("Cython must be installed to build this package (pip install Cython)")
        
        # Create build directory
        build_dir = Path(self.build_temp) / "cmake_build"
        build_dir.mkdir(parents=True, exist_ok=True)
        
        # CMake configure
        cmake_args = [
            '-DCMAKE_BUILD_TYPE=Release',
            f'-DPython3_EXECUTABLE={sys.executable}',
            '-DPython3_FIND_STRATEGY=LOCATION',         # optional: prefer the specified interpreter
            '-DPython3_FIND_VIRTUALENV=FIRST',           # optional: use active venv before system
        ]
        
        # Platform specific
        if sys.platform.startswith('win'):
            cmake_args.extend(['-G', 'Visual Studio 16 2019', '-A', 'x64'])
        
        print("Running CMake configure...")
        subprocess.check_call(['cmake', str(Path(__file__).parent)] + cmake_args, 
                            cwd=build_dir)
        
        # CMake build
        print("Running CMake build...")
        subprocess.check_call(['cmake', '--build', '.', '--config', 'Release'], 
                            cwd=build_dir)
        
        # Copy the built extension to the right place
        self.copy_extensions(build_dir)
    
    def copy_extensions(self, build_dir):
        """Copy built extensions to the package directory."""
        # Find the built extension
        extensions = list(build_dir.glob("reader.*"))
        if not extensions:
            extensions = list(build_dir.glob("**/reader.*"))
        
        if not extensions:
            raise RuntimeError("Could not find built extension module")
        
        extension_file = extensions[0]
        
        # Determine destination
        package_dir = Path(self.build_lib)
        package_dir.mkdir(parents=True, exist_ok=True)
        
        dest_file = package_dir / extension_file.name
        print(f"Copying {extension_file} -> {dest_file}")
        shutil.copy2(extension_file, dest_file)
        
        # Copy any dependent libraries from the reader directory
        reader_dir = build_dir / "reader"
        if reader_dir.exists():
            # Copy all dylib files from reader directory
            for lib_file in reader_dir.glob("*.dylib"):
                dest_lib_file = package_dir / lib_file.name
                print(f"Copying {lib_file} -> {dest_lib_file}")
                shutil.copy2(lib_file, dest_lib_file)
        
        # Copy any dependent libraries from lib dir (if it exists)
        lib_dir = build_dir / "lib"
        if lib_dir.exists():
            dest_lib_dir = package_dir / "lib"
            if dest_lib_dir.exists():
                shutil.rmtree(dest_lib_dir)
            shutil.copytree(lib_dir, dest_lib_dir)

# Minimal setup - the real work is done by CMake
setup(
    name="dftracer-reader",
    version="1.0.0",
    description="Python bindings for DFTracer gzip reader",
    ext_modules=[Extension("reader", sources=[])],  # Dummy extension
    cmdclass={'build_ext': CMakeBuild},
    zip_safe=False,
)
