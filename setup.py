from setuptools import setup, find_packages, Command
from setuptools.command.install import install
from webdriver_manager.chrome import ChromeDriverManager
from starstream import __version__

from pathlib import Path


class CustomInstallation(install):
    def run(self) -> None:
        self.run_command("install_chromedriver")
        install.run(self)


class InstallChromeDriverCommand(Command):
    description = "Install the appropriate ChromeDriver using webdriver-manager."
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            driver_path = ChromeDriverManager().install()
            print(f"ChromeDriver installed at: {driver_path}")
        except Exception as e:
            print(f"Failed to install ChromeDriver: {e}")
            raise


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
            "dateutil",
            "xarray",
            "tqdm",
            "numpy",
            "aiofiles",
            "beautifulsoup4",
            "selenium",
            "webdriver-manager",
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
        cmdclass={
            "install": CustomInstallation,
            "install_chromedriver": InstallChromeDriverCommand,
        },
    )
