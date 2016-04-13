#!/bin/bash

dirs=(./rpc ./fabnet ./logger)
echo "mode: set" > coverage.out
for Dir in ${dirs[*]};
do
        if ls $Dir/*.go &> /dev/null;
        then
            go test -coverprofile=profile.out $Dir
            if [ -f profile.out ]
            then
                cat profile.out | grep -v "mode: set" >> coverage.out
            fi
fi
done
rm profile.out
