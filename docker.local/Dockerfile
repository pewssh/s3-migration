FROM ubuntu:22.04 as s3mgrt-build

LABEL zchain="s3mgrt"

# https://mirrors.alpinelinux.org/
# RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN sudo apt update && sudo apt -y install build-essential nghttp2 libnghttp2-dev libssl-dev containerd docker.io
RUN curl -L -o go1.20.4.linux-amd64.tar.gz https://go.dev/dl/go1.20.4.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.20.4.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin
RUN go version

# # Install Herumi's cryptography
# WORKDIR /tmp

# COPY ./docker.local/bin/mcl.tar.gz ./
# COPY ./docker.local/bin/bls.tar.gz ./

# RUN tar zxvf mcl.tar.gz && rm mcl.tar.gz && mv mcl* mcl

# RUN tar zxvf bls.tar.gz && rm bls.tar.gz && mv bls* bls  

# RUN make -C mcl -j $(nproc) lib/libmclbn256.so install 
# RUN cp mcl/lib/libmclbn256.so /usr/local/lib 

# RUN make MCL_DIR=$(pwd)/mcl -C bls -j $(nproc) install 

# RUN rm -R /tmp/mcl && rm -R /tmp/bls

ENV SRC_DIR=/0chain
WORKDIR $SRC_DIR

# Download the dependencies:
# Will be cached if we don't change mod/sum files
COPY ./go.mod ./go.sum $SRC_DIR
RUN go mod download

COPY .  $SRC_DIR
RUN make build

FROM ubuntu:22.04
RUN sudo apt update && sudo apt -y install build-essential nghttp2 libnghttp2-dev libssl-dev containerd docker.io
COPY --from=s3mgrt-build  /usr/local/lib/libmcl*.so \
                        /usr/local/lib/libbls*.so \
                        /usr/local/lib/

COPY --from=s3mgrt-build /0chain/s3mgrt /usr/local/bin/s3mgrt