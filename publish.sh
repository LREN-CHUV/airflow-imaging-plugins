#!/usr/bin/env bash -e

# Build
./build.sh

count=$(git status --porcelain | wc -l)
if test $count -gt 0; then
  git status
  echo "Not all files have been committed in Git. Release aborted"
  exit 1
fi

# Look for a version tag in Git. If not found, ask the user to provide one
git describe --exact-match > /dev/null || (
  echo "The latest commit has not been tagged with a version. Please enter the version for this release."
  read -p "Version > " version
  git tag -a -m "PyPi release $version" $version
)

git push
git push --tags

# Push on PyPi
twine upload dist/*

# Notify on slack
set -e
get_script_dir () {
     SOURCE="${BASH_SOURCE[0]}"

     while [ -h "$SOURCE" ]; do
          DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
          SOURCE="$( readlink "$SOURCE" )"
          [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
     done
     cd -P "$( dirname "$SOURCE" )"
     pwd
}
export WORKSPACE=$(get_script_dir)
sed "s/USER/${USER^}/" $WORKSPACE/slack.json > $WORKSPACE/.slack.json
sed -i.bak "s/VERSION/$(git describe)/" $WORKSPACE/.slack.json
curl -k -X POST --data-urlencode payload@$WORKSPACE/.slack.json https://hbps1.chuv.ch/slack/dev-activity
rm -f $WORKSPACE/.slack.json
rm -f $WORKSPACE/.slack.json.bak
