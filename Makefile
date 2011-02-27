.PHONY: test clean

clean:
	find . -name "*.pyc" |xargs rm || true

test:
	py.test -svx test
