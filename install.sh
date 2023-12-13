#!/bin/bash

### Setup an environment to make the TnP analysis
sourceCommand="source /cvmfs/sft.cern.ch/lcg/views/LCG_104a/x86_64-el9-gcc11-opt/setup.sh"

eval "$sourceCommand"
python -m venv --system-site-packages myenv
source myenv/bin/activate

python -m pip install -e .[docs,dev]

python -m pip install --no-binary=correctionlib correctionlib

cat << EOF > start.sh
#!/bin/bash
$sourceCommand
source `pwd`/myenv/bin/activate
EOF

chmod +x start.sh
