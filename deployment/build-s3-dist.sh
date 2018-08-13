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
    echo "Error: Asset bucket not provided."
    exit 1
fi

if [ -z "$2" ]; then
    echo "Error: Solution version not provided."
    exit 1
fi

export solution_name="gaming-analytics-pipeline"

# Create `dist` directory
echo "Staring to build distribution"
echo "export deployment_dir=`pwd`"
export initial_dir=`pwd`
export deployment_dir="$initial_dir/deployment"
export dist_dir="$initial_dir/deployment/dist"
export source_dir="$initial_dir/source"
echo "Clean up $dist_dir"
rm -rf $dist_dir
echo "mkdir -p $dist_dir"
mkdir -p "$dist_dir"

# Copy CFT & swap parameters
cp "$deployment_dir/$solution_name.template" "$dist_dir/$solution_name.template"
echo "Updating code source bucket in template with $1"
replace="s/%%BUCKET_NAME%%/$1/g"
echo "sed -i '' -e $replace $dist_dir/$solution_name.template"
sed -i '' -e $replace "$dist_dir/$solution_name.template"

echo -e "\n Updating version number in the template with $2"
replace="s/%%VERSION%%/$2/g"
echo "sed -i '' -e $replace $dist_dir/$solution_name.template"
sed -i '' -e $replace "$dist_dir/$solution_name.template"

# Build Java Project
echo "Building Java Component"
mvn clean install -f "$source_dir/java/pom.xml"
cp "$source_dir/java/target/analytics-pipeline-1.0.0.war" "$dist_dir/analytics-pipeline-1.0.0.war"
echo "Cleaning up Java build"
mvn clean -f "$source_dir/java/pom.xml"

# Build Custom Resource
echo "Building CFN custom resource"
echo "Creating Python virtual environment"
TMP="$dist_dir/tmp"
virtualenv "$TMP/env"
source "$TMP/env/bin/activate"

echo "Installing custom-resource to virtual environment"
pip install "$source_dir/custom-resource" --target="$TMP/env/lib/python2.7/site-packages/"

echo "Creating Lambda zip package"
cd "$TMP/env/lib/python2.7/site-packages/"
zip -q -r9 "$dist_dir/custom-resource.zip" *
zip -q -d "./custom-resource.zip" "pip*" "easy*" "setup*" "wheel*" "pkg_resources*"
cd "$initial_dir"
echo "Cleaning up Python virtual environment"
rm -r "$TMP"

# Build Demo Assets
echo "Building data generators"
cp "$source_dir/tools-host/extract-tools.ps1" "$dist_dir/extract-tools.ps1"
cd "$source_dir/data-generator"
zip -r9 "$dist_dir/data-generator.zip" .
cd "$source_dir/heatmap-generator"
zip -r9 "$dist_dir/heatmap-generator.zip" .

cd "$initial_dir"

echo "Completed building distribution"
