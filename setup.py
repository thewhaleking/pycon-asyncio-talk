from setuptools import setup, Extension
from Cython.Build import cythonize
import os

setup_dir = os.path.dirname(os.path.abspath(__file__))
utils_dir = os.path.join(setup_dir, "utils")

extensions = [
    Extension(
        "utils.go_fast",
        [os.path.join(utils_dir, "go_fast.pyx")],
        language="c++",
        extra_compile_args=["-std=c++11", "-O3"],
    )
]

setup(
    ext_modules=cythonize(
        extensions,
        build_dir="utils",
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
        },
    ),
)
