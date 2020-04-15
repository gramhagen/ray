FROM ray-project/base-deps

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    curl \
    rsync \
    screen \
    tmux

# Install kubectl.
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN touch /etc/apt/sources.list.d/kubernetes.list
RUN echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update && apt-get install -y \
    kubectl

# We install ray and boto3 to enable the ray autoscaler.
RUN pip install -U pip
RUN pip install -U https://ray-wheels.s3-us-west-2.amazonaws.com/latest/ray-0.9.0.dev0-cp36-cp36m-manylinux1_x86_64.whl

# We install this after the latest wheels -- this should not override the latest wheels.
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN pip install torch==1.4.0+cpu torchvision==0.5.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow_cpu-2.1.0-cp36-cp36m-manylinux2010_x86_64.whl

ADD requirements.txt .
RUN pip install -r requirements.txt 

# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
