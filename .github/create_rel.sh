#!/usr/bin/env bash

pkgv=$(cargo pkgid | awk -F'[ #]' '{print "v"$2}')

ghv=$(gh release list -L 1 | awk -F" " '{print $1}')

# compare versions
if [[ $pkgv != $ghv ]]; then
    echo "'$pkgv' != '$ghv' ... creating new release tag"
    # create new rel tag
    $(gh release create "$pkgv"  --repo="$GITHUB_REPOSITORY" --title="$pkgv" --generate-notes)
else
    echo "$pkgv == $ghv ... stopping"
fi
