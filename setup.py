from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
from setuptools.command.egg_info import egg_info
import subprocess
from pathlib import Path

from starstream import __version__

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")


def get_requirements():
    return [
        "spacepy",
        "astropy",
        "xarray",
        "tqdm",
        "playwright",
        "numpy",
        "aiofiles",
        "beautifulsoup4",
        "pandas",
        "polars",
        "pillow",
        "torch",
        "joblib",
        "aiohttp",
        "viresclient",
    ]


def install_playwright():
    try:
        subprocess.run(["playwright", "install"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to install Playwright browsers: {e}")
    except FileNotFoundError:
        print(
            "Warning: Playwright command not found. Try running 'playwright install' manually."
        )


class CustomInstallCommand(install):
    def run(self):
        install.run(self)
        install_playwright()


class CustomDevelopCommand(develop):
    def run(self):
        develop.run(self)
        install_playwright()


class CustomEggInfoCommand(egg_info):
    def run(self):
        egg_info.run(self)
        install_playwright()


if __name__ == "__main__":
    setup(
        name="starstream",
        version=__version__,
        packages=find_packages(exclude=["tests*", "docs*"]),
        author="Jorge David Enciso Martínez",
        author_email="jorged.encyso@gmail.com",
        description="Asynchronous satellite data downloading for CDAWeb, JSOC, etc.",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/Jorgedavyd/starstream",
        license="MIT",
        python_requires=">=3.9",
        install_requires=get_requirements(),
        extras_require={
            "dev": [
                "pytest",
                "sphinx",
                "black",
                "isort",
                "mypy",
                "flake8",
            ],
        },
        cmdclass={
            "install": CustomInstallCommand,
            "develop": CustomDevelopCommand,
            "egg_info": CustomEggInfoCommand,
        },
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Science/Research",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: Implementation :: CPython",
            "Programming Language :: Python :: Implementation :: PyPy",
            "Operating System :: Microsoft :: Windows",
            "Operating System :: POSIX :: Linux",
            "Operating System :: MacOS :: MacOS X",
            "Topic :: Scientific/Engineering",
            "Topic :: Scientific/Engineering :: Astronomy",
            "Topic :: Scientific/Engineering :: Physics",
            "Topic :: Scientific/Engineering :: Mathematics",
            "Topic :: Scientific/Engineering :: Visualization",
            "Topic :: Scientific/Engineering :: Image Processing",
            "Topic :: Scientific/Engineering :: Information Analysis",
            "Topic :: Scientific/Engineering :: Artificial Intelligence",
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Framework :: Matplotlib",
            "Framework :: Pytest",
            "Framework :: Sphinx",
            "Framework :: Jupyter",
            "Framework :: IPython",
            "Environment :: Console",
            "Environment :: Web Environment",
            "Natural Language :: English",
            "Typing :: Typed",
        ],
        project_urls={
            "Bug Reports": "https://github.com/Jorgedavyd/starstream/issues",
            "Source": "https://github.com/Jorgedavyd/starstream",
            "Documentation": "https://starstream.readthedocs.io/",
        },
    )
