#!/bin/sh
#find . -name pom.xml -execdir sed s/com\.xeiam\.xchange/com\.hftc\.xchange/g {} \; \> pom2.xml \;
perl -e "s/com\.xeiam\.xchange/com\.hftc\.xchange/g;" -pi $(find . -name pom.xml)
