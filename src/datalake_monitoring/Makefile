.PHONY: bundle
bundle:
	rm -rf ./python && mkdir -p ./python
	find . -type d -name __pycache__ -exec rm -r {} \+
	poetry export -f requirements.txt --without-hashes > requirements-poetry.txt
	cat requirements-poetry.txt
	pip install -r requirements-poetry.txt --target ./python
	rm -rf ./python/*dist-info*
	curl -L -C - -o ./libs/awswrangler-layer-3.2.0-py3.8.zip https://github.com/awslabs/aws-data-wrangler/releases/download/3.2.0/awswrangler-layer-3.2.0-py3.8.zip


.PHONY: clean
clean:
	rm -rf python/ requirements-poetry.txt
