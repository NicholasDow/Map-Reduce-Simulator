import os
from setuptools import setup, find_packages

install_requires = []

requirement_path = os.path.dirname(
    os.path.realpath(__file__)) + "/requirements.txt"

if os.path.isfile(requirement_path):
    with open(requirement_path) as f:
        install_requires = f.read().splitlines()

setup(
    name='distributed_sim',
    setup_requires=['wheel'],
    install_requires=install_requires,
    packages=find_packages(),
)
