from setuptools import setup, find_packages

setup(
    name="pycernan",
    version="0.0.7",
    author="John Koenig",
    author_email="john@postmates.com",
    description="Python client for Cernan.",
    license="MIT",
    keywords="client cernan",
    url="https://github.com/postmates/pycernan",
    packages=find_packages(),
    tests_require=[
        'pytest',
        'pytest-cov',
        'pytest-timeout',
        'mock>=1.0.1',
    ],
    install_requires=[
    ],
    extras_require={
        ":python_version<'3.0'": ["avro"],
        ":python_version>='3.0'": ["avro-python3"],
    },
    dependency_links=[
        "git+git://github.com/postmates/avro.git#subdirectory=lang/py"
    ],
    include_package_data=True,
    scripts=[],
    classifiers=[
        "Topic :: Utilities",
    ],
)
