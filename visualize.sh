#!/bin/bash
# Run after evaluation and visualization pipeline
# Takes in .tex graphs and converts them
# Sync paths with docker-compose on host machine

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