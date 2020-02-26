#!/bin/sh
rm dist/ray-0.9.0.dev0-cp37-cp37m-linux_x86_64.whl
python setup.py bdist_wheel
az storage blob upload --account-name marcozopip -f dist/ray-0.9.0.dev0-cp37-cp37m-linux_x86_64.whl -c wheel -n ray-0.9.0.dev0-cp37-cp37m-linux_x86_64.whl
ray up -y /home/marcozo/ray/python/ray/autoscaler/azure/example-full-marcozo.yaml

echo Run "az group delete -y --no-wait -n mc-ray-cluster02" for cleanup
