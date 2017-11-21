#!/bin/bash

# This assumes all of the OS-level configuration has been completed and git repo has already been cloned
# sudo apt-get zip
# sudo pip install --upgrade virtualenv

# This script should be run from the repo's deployment directory
# cd deployment
# ./build-s3-dist.sh source-bucket-base-name
# source-bucket-base-name should be the base name for the S3 bucket location where the template will source the Lambda code from.
# The template will append '-[region_name]' to this bucket name.
# For example: ./build-s3-dist.sh solutions
# The template will then expect the source code to be located in the solutions-[region_name] bucket

# Check to see if input has been provided:
if [ -z "$1" ]; then
    echo "Please provide the base source bucket name where the lambda code will eventually reside.\nFor example: ./build-s3-dist.sh solutions"
    exit 1
fi

# Build source
echo "Staring to build distribution"
echo "export deployment_dir=`pwd`"
export deployment_dir=`pwd`
echo "mkdir -p dist"
mkdir -p dist

echo "Creating CFN template"
echo "Updating code source bucket in template with $1"
REPLACE="s/%%BUCKET_NAME%%/$1/g"
cp "./gaming-analytics-pipeline.yaml" "./dist/gaming-analytics-pipeline.yaml"
sed -i '' -e $REPLACE "./dist/gaming-analytics-pipeline.yaml"

echo "Building Java Component"
mvn clean install -f "../source/java/pom.xml"
cp "../source/java/target/analytics-pipeline-1.0.0.war" "./dist/analytics-pipeline-1.0.0.war"
echo "Cleaning up Java build"
mvn clean -f "../source/java/pom.xml"

echo "Building CFN custom resource"
echo "Creating Python virtual environment"
TMP="./dist/tmp"
virtualenv "$TMP/env"
source "$TMP/env/bin/activate"
echo "Installing custom-resource to virtual environment"
pip install "../source/custom-resource" --target="$TMP/env/lib/python2.7/site-packages/"
echo "Creating lambda zip package"
cd "$TMP/env/lib/python2.7/site-packages/"
zip -r9 "./custom-resource.zip" .
zip -q -d "./custom-resource.zip" pip*
zip -q -d "./custom-resource.zip" easy*
cp "./custom-resource.zip" "$deployment_dir/dist/custom-resource.zip"
cd $deployment_dir
echo "Cleaning up Python virtual environment"
rm -r "$TMP"

echo "Building data generators"
cp "../source/tools-host/extract-tools.ps1" "./dist/extract-tools.ps1"
cd "../source/data-generator"
zip -r9 "$deployment_dir/dist/data-generator.zip" .
cd "../heatmap-generator"
zip -r9 "$deployment_dir/dist/heatmap-generator.zip" .

echo "Completed building distribution"
