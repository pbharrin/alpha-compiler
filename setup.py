#!/usr/bin/env python

from distutils.core import setup

setup(name='Alpha Compiler',
      version='0.1',
      description='Python tools for quantitative finance',
      author='Peter Harrington',
      author_email='peter.b.harrington@gmail.com',
      url='https://www.alphacompiler.com/',
      packages=['alphacompiler', 'alphacompiler.compiler', 
          'alphacompiler.util', 'alphacompiler.data', 'alphacompiler.data.loaders'],
     )
