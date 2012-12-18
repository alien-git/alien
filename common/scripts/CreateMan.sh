#!/bin/bash 

OUTPUTDIR="$HOME/.alien/AliEn/Html/Doc/man"

INPUTDIR="$HOME/.alien/AliEn"


echo "Doing the man pages (and keeping them in $OUTPUTDIR)"

cd  $INPUTDIR
for n in `find ./ -type d`;  do
  if [ -z ${n##./Html*} ] ; then
    echo "Skipping $n "
  else
    if [ -z ${n##./doc*} ] ; then
      echo "Skipping $n"
    else
      if [ ! -d "$OUTPUTDIR/$n" ] ; then  
	echo "DIRECTORY $OUTPUTDIR/$n does not exist... creating it"
	mkdir -p "$OUTPUTDIR/$n" || echo "ERROR $? $!" 
      fi
    fi
  fi
done


echo
echo "\nCreating the manpages"


for n in `find ./  -name "*.pm"`;  do
  if [ -z ${n##./Html*} ] ; then
    echo "Skipping $n "
  else
    if [ -z ${n##./doc*} ] ; then
      echo "Skipping $n"
    else
      echo "Creating $OUTPUTDIR/$n " 
      pod2man $n > $OUTPUTDIR/$n
      pod2html $n > $OUTPUTDIR/${n%.pm}.html
    fi
  fi
done

