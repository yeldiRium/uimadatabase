#!/bin/bash
set -e

if [ ! -d ~/nlpdbeval_data ]
then
	mkdir ~/nlpdbeval_data
fi

if [ ! -d ~/nlpdbeval_data/input ]
then
	mkdir ~/nlpdbeval_data/input
	echo "Please add some input files and set appropriate configuration values."
fi

if [ ! -d ~/nlpdbeval_data/output ]
then
	mkdir ~/nlpdbeval_data/output
fi

mvn clean package
docker-compose build
docker-compose run main java -cp /code/target.jar org.hucompute.services.uima.eval.main.EvaluationPipeline
docker-compose run main java -cp /code/target.jar org.hucompute.services.uima.eval.main.VisualizationPipeline

if [ -d ~/nlpdbeval_data/pdfs ]
then
	rm -r ~/nlpdbeval_data/pdfs
fi
mkdir ~/nlpdbeval_data/pdfs

cd $(mktemp -d)
for filename in ~/nlpdbeval_data/output/graphs/*.tex
do
	pdflatex -interaction batchmode $filename >/dev/null
	echo "$filename converted"
done

mv *.pdf ~/nlpdbeval_data/pdfs
rm *.log
rm *.aux
cd -