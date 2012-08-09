clean-pyc:
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

documentation:
	cd docs && make clean html

pep8:
	pep8 -r pyvertica && echo "All good!"

unittest: clean-pyc
	coverage erase
	coverage run --include "pyvertica*" --omit "*test*" -m unittest2 discover
	coverage report

test: pep8 unittest documentation
