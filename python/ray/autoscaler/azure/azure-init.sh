#!/bin/sh

USERNAME=$1
CONDA_ENV=$2
WHEEL=$3
RAY_HEAD_IP=$4
TYPE=$5

echo "Installing wheel..."
sudo -u $USERNAME -i /bin/bash -l -c "conda init bash"
sudo -u $USERNAME -i /bin/bash -l -c "conda activate $CONDA_ENV; pip install $WHEEL"

echo "Setting up service scripts..."
cat > /home/$USERNAME/ray-head.sh << EOM
#/bin/sh
conda activate $CONDA_ENV

NUM_GPUS=\`nvidia-smi -L | wc -l\`

ray stop
ulimit -n 65536 ; ray start --head --redis-port=6379 --object-manager-port=8076 --num-gpus=\$NUM_GPUS --block
EOM

cat > /home/$USERNAME/ray-worker.sh << EOM
#/bin/sh
conda activate $CONDA_ENV

NUM_GPUS=\`nvidia-smi -L | wc -l\`

ray stop
ulimit -n 65536 ;  ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076 --num-gpus=\$NUM_GPUS --block
EOM

chmod +x /home/$USERNAME/ray-head.sh
chmod +x /home/$USERNAME/ray-worker.sh

cat > /lib/systemd/system/ray.service << EOM
[Unit]
   Description=Ray

[Service]
   Type=simple
   User=$USERNAME
   ExecStart=/bin/bash -l /home/$USERNAME/ray-$TYPE.sh

[Install]
WantedBy=multi-user.target
EOM

echo "Configure ray to start at boot..."
systemctl enable ray

echo "Starting ray..."
systemctl start ray