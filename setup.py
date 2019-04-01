from setuptools import setup, find_packages


def main():
    setup(
        name='gce-task-runner',
        version='1.2.1',
        author='rei.suyama',
        author_email='rei.suyama@sensy.ai',
        maintainer='rei.suyama',
        maintainer_email='rei.suyama@sensy.ai',
        include_package_data=True,
        packages=find_packages(),
        install_requires=[
            'requests >= 2.21.0',
            'google-api-python-client >= 1.7.8',
            'google-cloud-pubsub >= 0.39.1',
            'asynconsumer == 1.0.2'
        ],
        extras_require={
        },
        classifiers=[
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
        ],
    )


if __name__ == '__main__':
    main()
