#!/usr/bin/env bash

set -e

# Build
./build.sh

count=$(git status --porcelain | wc -l)
if test $count -gt 0; then
  git status
  echo "Not all files have been committed in Git. Release aborted"
  exit 1
fi

select_part() {
  local choice=$1
  case "$choice" in
      "Patch release*")
          part='patch'
          ;;
      "Minor release*")
          part='minor'
          ;;
      "Major release*")
          part='major'
          ;;
      *)
          read -p "Version > " part
          ;;
  esac
}

# Look for a version tag in Git. If not found, ask the user to provide one
git describe --exact-match > /dev/null || (
  latest_version=$(git describe --abbrev=0)
  echo "Current commit has not been tagged with a version. Latest known version is $latest_version."
  PS3='What do you want to release?'
  options=("Patch release (increments version to x.x.N)" "Minor release (increments version to x.N)" "Major release (increments to N.0)" "Release with a custom version")
  select choice in "${options[@]}";
  do
    select_part "$choice"
    break
  done
  bumpversion --tag --tag-name 'PyPI release {new_version}' $part
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
