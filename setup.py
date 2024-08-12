from setuptools import setup, find_packages

from starstream import __version__

from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

if __name__ == "__main__":
    setup(
        name="starstream",
        version=__version__,
        packages=find_packages(),
        author="Jorge David Enciso Martínez",
        long_description=long_description,
        long_description_content_type="text/markdown",
        author_email="jorged.encyso@gmail.com",
        description="Asynchronous satellite data downloading for CDAWeb, JSOC, etc.",
        url="https://github.com/Jorgedavyd/starstream",
        license="MIT",
        install_requires=[
            "spacepy",
            "astropy",
            "xarray",
            "tqdm",
            "numpy",
            "aiofiles",
            "beautifulsoup4",
            "selenium==4.18.1",
            "chromedriver-binary==125.0.6422.76.0",
            "pandas",
            "pillow",
            "joblib",
            "aiohttp",
            "viresclient",
            "asyncio",
        ],
        classifiers=[
            "Development Status :: 4 - Beta",
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
    )
