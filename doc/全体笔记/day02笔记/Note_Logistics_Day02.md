---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day02：Docker 基本使用

[每个知识点学习目标：了解（know）、理解（understand）、掌握（grasp）、复习（review）]()

![img](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/u=3762540439,4180220111&fm=26&gp=0.jpg)

## 01-[复习]-上次课程内容回顾

> 主要2个部分内容：项目概述与解决方案、Docker 容器引擎入门。

![1612141361265](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612141361265.png)



## 02-[了解]-第2天：课程内容提纲

> 继续讲解Docker 基本使用，主要讲解命令（镜像和容器）：

![1612141446643](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612141446643.png)

> - 1）、Docker 常用命令
>   - 帮助命令：--help
>   - 镜像命令（四五个）
>   - 容器命令（四五类命令）
>     - 创建与删除及查看、启动与停止、进入容器、端口号及Ip地址及磁盘挂载等等、备份
> - 2）、Docker 容器部署应用
>   - MySQL数据库、Nginx服务器和Redis数据库
> - 3）、Docker 迁移和备份
>   - 镜像 -> 容器 -> 镜像镜像 -> 持久化tar文件 -> 加载为镜像 -> 创建容器
> - 4）、Docker 镜像
> - 5）、Dockerfile
>   - 编写Docker 镜像文件Dockerfile，使用build命令构建镜像
> - 6）、Docker 私有仓库

## 03-[掌握]-Docker 帮助命令

> 学习任何框架软件，都会提供主要命令脚本，以供使用，通常情况下：`--help`，告知命令如何使用

![1612142085822](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612142085822.png)

> 查看docker命令使用帮助

```ini
[root@node1 ~]# docker --help

Usage:  docker [OPTIONS] COMMAND

A self-sufficient runtime for containers

Options:
      --config string      Location of client config files (default "/root/.docker")
  -c, --context string     Name of the context to use to connect to the daemon (overrides DOCKER_HOST env var and default
                           context set with "docker context use")
  -D, --debug              Enable debug mode
  -H, --host list          Daemon socket(s) to connect to
  -l, --log-level string   Set the logging level ("debug"|"info"|"warn"|"error"|"fatal") (default "info")
      --tls                Use TLS; implied by --tlsverify
      --tlscacert string   Trust certs signed only by this CA (default "/root/.docker/ca.pem")
      --tlscert string     Path to TLS certificate file (default "/root/.docker/cert.pem")
      --tlskey string      Path to TLS key file (default "/root/.docker/key.pem")
      --tlsverify          Use TLS and verify the remote
  -v, --version            Print version information and quit

Management Commands:
  app*        Docker App (Docker Inc., v0.9.1-beta3)
  builder     Manage builds
  buildx*     Build with BuildKit (Docker Inc., v0.5.1-docker)
  config      Manage Docker configs
  container   Manage containers
  context     Manage contexts
  image       Manage images
  manifest    Manage Docker image manifests and manifest lists
  network     Manage networks
  node        Manage Swarm nodes
  plugin      Manage plugins
  secret      Manage Docker secrets
  service     Manage services
  stack       Manage Docker stacks
  swarm       Manage Swarm
  system      Manage Docker
  trust       Manage trust on Docker images
  volume      Manage volumes

Commands:
  attach      Attach local standard input, output, and error streams to a running container
  build       Build an image from a Dockerfile
  commit      Create a new image from a container's changes
  cp          Copy files/folders between a container and the local filesystem
  create      Create a new container
  diff        Inspect changes to files or directories on a container's filesystem
  events      Get real time events from the server
  exec        Run a command in a running container
  export      Export a container's filesystem as a tar archive
  history     Show the history of an image
  images      List images
  import      Import the contents from a tarball to create a filesystem image
  info        Display system-wide information
  inspect     Return low-level information on Docker objects
  kill        Kill one or more running containers
  load        Load an image from a tar archive or STDIN
  login       Log in to a Docker registry
  logout      Log out from a Docker registry
  logs        Fetch the logs of a container
  pause       Pause all processes within one or more containers
  port        List port mappings or a specific mapping for the container
  ps          List containers
  pull        Pull an image or a repository from a registry
  push        Push an image or a repository to a registry
  rename      Rename a container
  restart     Restart one or more containers
  rm          Remove one or more containers
  rmi         Remove one or more images
  run         Run a command in a new container
  save        Save one or more images to a tar archive (streamed to STDOUT by default)
  search      Search the Docker Hub for images
  start       Start one or more stopped containers
  stats       Display a live stream of container(s) resource usage statistics
  stop        Stop one or more running containers
  tag         Create a tag TARGET_IMAGE that refers to SOURCE_IMAGE
  top         Display the running processes of a container
  unpause     Unpause all processes within one or more containers
  update      Update configuration of one or more containers
  version     Show the Docker version information
  wait        Block until one or more containers stop, then print their exit codes

Run 'docker COMMAND --help' for more information on a command.

To get more help with docker, check out our guides at https://docs.docker.com/go/guides/
```



## 04-[掌握]-Docker 镜像命令

![1612142461163](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612142461163.png)

> - 1）、搜索镜像
>   - 方式一：https://hub.docker.com/，网站直接搜索
>   - 方式二：`docker search 某个XXX镜像名字`
> - 2）、拉取镜像
>   - 命令：`docker pull 镜像名称[:tag]`

![1612142748536](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612142748536.png)

> - 3）、查看镜像（本地拉取镜像或构件镜像）
>   - 命令：`docker images`
> - 4）、删除镜像
>   - 命令：`docker rmi -f 镜像ID`



## 05-[掌握]-Docker 容器命令之查看与创建

> - 1）、查看容器
>   - 使用最多命令：`docker ps  或者 docker ps -a`

![1612143821928](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612143821928.png)



> - 2）、创建与启动容器
>   - 方式一：创建守护式容器（后台运行）
>   - 方式二：创建交互式容器（前端运行）

![1612144069882](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612144069882.png)

> 采用交互式方式创建容器，创建完成以后，直接进入容器

```ini
[root@node1 ~]# docker run -it --name=centos centos:7 /bin/bash
[root@9f3b72b4073a /]# 
```



> 守护式方式创建容器，创建完成以后，需要自己进入容器，才能操作

```ini
[root@node1 ~]# docker run -di --name=mycentos7 8652b9f0cb4c
27d2e1f0659490b2dfe3c4fc4104a523ac3cdbfe1f23bcef70c30b1c5bf21f27

[root@node1 ~]# 
[root@node1 ~]# docker ps
CONTAINER ID   IMAGE          COMMAND       CREATED         STATUS         PORTS     NAMES
27d2e1f06594   8652b9f0cb4c   "/bin/bash"   4 seconds ago   Up 2 seconds             mycentos7
```

> 登录守护式容器方式：

```ini
[root@node1 ~]# docker exec -it 27d2e1f06594 /bin/bash
[root@27d2e1f06594 /]# 
```



> 容器删除：`docker rm 容器名称（容器ID）`，删除容器时，要确定容器没有运行，必须停止

```ini
[root@node1 ~]# docker ps -a
CONTAINER ID   IMAGE          COMMAND            CREATED         STATUS                      PORTS     NAMES
27d2e1f06594   8652b9f0cb4c   "/bin/bash"        3 minutes ago   Up 3 minutes                          mycentos7
475811ee0e8d   8652b9f0cb4c   "/bin/bash"        4 minutes ago   Exited (0) 4 minutes ago              centos7
df6229e56f90   8652b9f0cb4c   "--name=centos7"   4 minutes ago   Created                               magical_dubinsky
9f3b72b4073a   centos:7       "/bin/bash"        7 minutes ago   Exited (0) 5 minutes ago              centos
ac50485e9533   centos:7       "/bin/bash"        17 hours ago    Exited (137) 17 hours ago             mycentos
[root@node1 ~]# 
# 指定容器ID删除容器
[root@node1 ~]# docker rm 475811ee0e8d
475811ee0e8d
[root@node1 ~]# docker ps -a          
CONTAINER ID   IMAGE          COMMAND            CREATED         STATUS                      PORTS     NAMES
27d2e1f06594   8652b9f0cb4c   "/bin/bash"        4 minutes ago   Up 4 minutes                          mycentos7
df6229e56f90   8652b9f0cb4c   "--name=centos7"   5 minutes ago   Created                               magical_dubinsky
9f3b72b4073a   centos:7       "/bin/bash"        9 minutes ago   Exited (0) 7 minutes ago              centos
ac50485e9533   centos:7       "/bin/bash"        17 hours ago    Exited (137) 17 hours ago             mycentos
[root@node1 ~]# 
# 指定容器名，删除容器
[root@node1 ~]# docker rm magical_dubinsky
magical_dubinsky
[root@node1 ~]# 
[root@node1 ~]# docker ps -a
CONTAINER ID   IMAGE          COMMAND       CREATED         STATUS                      PORTS     NAMES
27d2e1f06594   8652b9f0cb4c   "/bin/bash"   4 minutes ago   Up 4 minutes                          mycentos7
9f3b72b4073a   centos:7       "/bin/bash"   9 minutes ago   Exited (0) 7 minutes ago              centos
ac50485e9533   centos:7       "/bin/bash"   17 hours ago    Exited (137) 17 hours ago             mycentos
```



## 06-[掌握]-Docker 容器命令之启动与停止

> - 1）、容器停止

![1612144844629](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612144844629.png)

> - 2）、启动容器

![1612144890440](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612144890440.png)



```ini
[root@node1 ~]# docker stop mycentos7
mycentos7
[root@node1 ~]# 
[root@node1 ~]# docker ps -a         
CONTAINER ID   IMAGE          COMMAND       CREATED          STATUS                        PORTS     NAMES
27d2e1f06594   8652b9f0cb4c   "/bin/bash"   7 minutes ago    Exited (137) 23 seconds ago             mycentos7
9f3b72b4073a   centos:7       "/bin/bash"   11 minutes ago   Exited (0) 10 minutes ago               centos
ac50485e9533   centos:7       "/bin/bash"   17 hours ago     Exited (137) 17 hours ago               mycentos
[root@node1 ~]# 
[root@node1 ~]# docker start centos
centos
[root@node1 ~]# docker ps
CONTAINER ID   IMAGE      COMMAND       CREATED          STATUS         PORTS     NAMES
9f3b72b4073a   centos:7   "/bin/bash"   12 minutes ago   Up 3 seconds             centos
[root@node1 ~]# 
[root@node1 ~]# 
[root@node1 ~]# docker exec -it centos /bin/bash
[root@9f3b72b4073a /]# 
```



> - 3）、重启容器

![1612144961186](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612144961186.png)



> - 4）、强制停止容器

![1612145010382](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612145010382.png)



```ini
[root@node1 ~]# docker ps
CONTAINER ID   IMAGE      COMMAND       CREATED          STATUS          PORTS     NAMES
9f3b72b4073a   centos:7   "/bin/bash"   13 minutes ago   Up 59 seconds             centos
[root@node1 ~]# 
[root@node1 ~]# 
[root@node1 ~]# docker restart 9f3b72b4073a
9f3b72b4073a
[root@node1 ~]# docker ps
CONTAINER ID   IMAGE      COMMAND       CREATED          STATUS          PORTS     NAMES
9f3b72b4073a   centos:7   "/bin/bash"   14 minutes ago   Up 34 seconds             centos
[root@node1 ~]# 
[root@node1 ~]# docker kill 9f3b72b4073a
9f3b72b4073a
[root@node1 ~]# 
[root@node1 ~]# docker ps
CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES
```



## 07-[掌握]-Docker 容器命令之文件拷贝

> 文件拷贝，将宿主机文件拷贝至容器中，或者将容器中文件拷贝至宿主机。

![1612145342077](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612145342077.png)

> - 方式一：从外到里（宿主机文件至容器中）
>
>   `docker cp 需要拷贝的文件或者目录 容器名称:容器目录`
>
> - 方式二：从里到外（容器中至宿主机）
>
>   `docker cp 容器名称:容器目录 需要拷贝的文件或者目录`

```ini
[root@node1 ~]# docker cp /root/hello.txt centos:/root

[root@node1 ~]# docker cp centos:/root/docker.txt /root
```



## 08-[掌握]-Docker 容器命令之目录挂载

> 目录挂载，使得容器与宿主机共享文件系统，使得使用文件时，就不需要cp拷贝，直接使用即可，更加方便

![1612145820915](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612145820915.png)

> ​		可以在创建容器的时候，将宿主机的目录和容器内的目录进行映射，这样就可以通过修改宿主机的某个目录的文件从而去影响容器。
>
> 命令：`创建容器添加-v参数，后边为宿主机目录:容器目录`

![1612145870845](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612145870845.png)

## 09-[掌握]-Docker 容器命令之IP 地址

> 每个容器运行时，都有一个内部Ip地址，可以通过命令查看：
>
> `docker inspect 容器名称（容器id）`

```ini
[root@node1 ~]# docker inspect mycentos
.............
"Networks": {
                "bridge": {
                    "IPAMConfig": null,
                    "Links": null,
                    "Aliases": null,
                    "NetworkID": "cb11670bbe5e7b7e4f1b697e477602b3c75948e52230e63cd9a52cc7cc696eec",
                    "EndpointID": "6a4a26090ddf76262df617810fabbab70630b835c691057fd424040c95fc02e9",
                    "Gateway": "172.17.0.1",
                    "IPAddress": "172.17.0.2",
                    "IPPrefixLen": 16,
                    "IPv6Gateway": "",
                    "GlobalIPv6Address": "",
                    "GlobalIPv6PrefixLen": 0,
                    "MacAddress": "02:42:ac:11:00:02",
                    "DriverOpts": null
                }
            }
            
[root@node1 ~]# docker inspect --format=’{{.NetworkSettings.IPAddress}}’ centos
’172.17.0.2’
[root@node1 ~]# 
[root@node1 ~]# docker inspect --format=’{{.NetworkSettings.IPAddress}}’ centos7
’172.17.0.3’     
```



> 也可以进入容器中，安装net-tools工具软件，使用ifconfig查看IP地址

```ini
# 安装nettools软件
[root@9f3b72b4073a /]# yum -y install net-tools

[root@9f3b72b4073a /]# ifconfig
eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 172.17.0.2  netmask 255.255.0.0  broadcast 172.17.255.255
        ether 02:42:ac:11:00:02  txqueuelen 0  (Ethernet)
        RX packets 1804  bytes 12259113 (11.6 MiB)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 1712  bytes 96548 (94.2 KiB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

lo: flags=73<UP,LOOPBACK,RUNNING>  mtu 65536
        inet 127.0.0.1  netmask 255.0.0.0
        loop  txqueuelen 1000  (Local Loopback)
        RX packets 0  bytes 0 (0.0 B)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 0  bytes 0 (0.0 B)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
```



## 10-[理解]-Docker 应用部署之MySQL

> 使用Docker 容器部署应用步骤：
>
> - 1）、搜索镜像
> - 2）、拉取镜像
> - 3）、查看镜像
> - 4）、创建容器，守护式容器，启动运行容器
> - 5）、进入容器，启动服务（容器启动时，就会运行服务，比如MySQL数据库启动）
> - 6）、容器运行服务使用

```ini
docker search mysql

docker pull centos/mysql-57-centos7

docker run -di --name=mysql57 -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 centos/mysql-57-centos7
```

> 创建容器时，需要映射端口号，将容器中端口号映射到宿主机端口号，方便进行访问

![1612148488052](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612148488052.png)

## 11-[理解]-Docker 应用部署之Nginx

> 操作如下所示：

```ini
docker search nginx

# 拉取镜像属于最新版本
docker pull nginx

docker run -di --name=mynginx -p 80:80 nginx

# 打开浏览器访问
http://node1.itcast.cn/
```

![1612149080462](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612149080462.png)



## 12-[理解]-Docker 应用部署之Redis

> 操作命令如下：

```ini
docker pull redis:5

# 创建容器
docker run -di --name=myredis -p 6379:6379 redis:5

# 进入容器，直接登录redis cli命令行
docker exec -it myredis redis-cli
```

![1612149430508](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612149430508.png)

> 再次创建Redis容器，需要指定Redis服务配置文件和数据存储目录挂载到宿主机上，示意图如下；

![1612149700668](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612149700668.png)

```ini
# 需要关闭前面myredis容器，可以将其删除，原因在于端口号冲突
docker run -p 6379:6379 -v /export/docker/myredis/data/:/data -v /export/docker/myredis/conf/redis.conf:/usr/local/etc/redis/redis.conf -d redis:5 redis-server /usr/local/etc/redis/redis.conf --appendonly yes
```



## 13-[掌握]-Docker  容器迁移与备份

> 当使用Docker 容器部署运行应用时，在不同环境运行容器，可以进行Docker 迁移和备份。

![1612151361138](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612151361138.png)

> - 1）、容器保存为镜像
>
>   - 将容器Contanier提交为镜像image
>
>   `docker commit mynginx mynginx_image`
>
> - 2）、镜像备份
>
>   - 将镜像保存为tar文件，镜像持久化
>
>   `docker save -o mynginx.tar mynginx_image`
>
> - 3）、镜像恢复
>
>   - 将tar文件加载为镜像image
>
>   `docker load -i mynginx.tar`
>
> - 4）、创建容器
>
>   - 从加载出镜像来创建容器Contanier
>
>   `docker run -di --name=mynginx2 -p 81:80 mynginx_image`

## 14-[理解]-Docker 镜像是什么

> ​		==镜像Image==是一种轻量级、可执行的独立软件包，用来打包软件运行环境和基于运行环境开发的软件，它包含运行某个软件所需的所有内容，包括代码、运行时、库、环境变量和配置文件。
>
> [Union文件系统（UnionFS）是一种分层、轻量级并且高性能的文件系统，它支持对文件系统的修改作为一次提交来一层层的叠加]()
>
> ​		Union 文件系统是 Docker 镜像的基础。镜像可以通过分层来进行继承，基于基础镜像（没有父镜像），可以制作各种具体的应用镜像。

![1612152145321](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612152145321.png)

> Docker 镜像加载原理：先加载bootfs，再加载rootfs。

![1612152270455](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612152270455.png)

> 分层的镜像，以pull为例，在下载的过程中我可以看到docker的镜像好像是在一层一层的在下载

![1612152369502](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612152369502.png)

> Docker 镜像特点:
>
> - Docker镜像都是只读的，当容器启动时，一个新的可写层被加载到镜像的顶部
> - 当从镜像创建容器运行时，其实就是在镜像文件系统之上加了可读可写一次文件系统：`Writeable Contanier 文件系统层`。

![1612152440372](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612152440372.png)



## 15-[理解]-什么是 Dockerfile

![1612161322972](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612161322972.png)

> 从上图中可以看出，Docker中镜像非常关键，从哪里来的，能够得到什么（容器）：
>
> - 1）、镜像Image由**Dockerfile**文件`build`（构建）而来；
> - 2）、容器**Contanier**从镜像Image创建`run`而来；
>
> [查看CentOS:7镜像Dockerfile文件，其中内容分为4个步骤进行操作，如下图所示：]()

![1612161980479](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612161980479.png)

> Dockerfile是由一系列命令和参数构成的脚本，这些命令应用于基础镜像并最终创建一个新的镜像。
>
> ==构建Dockerfile三个步骤==

![1612162138591](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612162138591.png)



## 16-[掌握]-Dockerfile 构建过程解析

> Docker 执行Dockerfile大致流程

![1612162657275](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612162657275.png)

> ​			Dockerfile面向开发，Docker镜像成为交付标准，Docker容器则涉及部署与运维，三者缺一不可，合力充当Docker体系的基石。

![1612162689368](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612162689368.png)



## 17-[理解]-Dockerfile 常用命令

![1612162826512](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612162826512.png)



## 18-[理解]-Dockerfile 案例之创建镜像JDK

> 编写Dockerfile镜像文件，基于CentOS:7镜像安装JDK8软件，并且设置环境变量JAVA_HOME。

![1612163417207](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612163417207.png)

> Dockerfile文件内容如下；

```ini
#依赖镜像名称和id
FROM centos:7

#指定镜像创建者信息
MAINTAINER ITCAST

#切换工作目录
WORKDIR /usr
RUN mkdir -p /usr/local/java

#ADD 是相对路径jar，把java添加到容器中
ADD jdk-8u221-linux-x64.tar.gz /usr/local/java

#配置java环境变量
ENV JAVA_HOME /usr/local/java/jdk1.8.0_221
ENV JRE_HOME $JAVA_HOME/jre
ENV CLASSPATH $JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$JRE_HOME/lib:$CLASSPATH
ENV PATH $JAVA_HOME/bin:$PATH
```

> 执行命令，从Dockerfile构建镜像：
>
> `docker build -t='jdk1.8' .`
>
> ==注意后面的空格和点，不要省略，点表示当前目录==

基于构建镜像，创建容器并运行，命令如下所示：

```ini
[root@node1 jdk8]# docker run -di --name=jdk8 de7c8c725d6d
29ef174768d300e7bc2bdd55de989dbb45c8c25b0ee88a9e00560e0c78bd98c2
[root@node1 jdk8]# 
[root@node1 jdk8]# docker ps
CONTAINER ID   IMAGE          COMMAND       CREATED         STATUS         PORTS     NAMES
29ef174768d3   de7c8c725d6d   "/bin/bash"   3 seconds ago   Up 2 seconds             jdk8

[root@node1 jdk8]# 
[root@node1 jdk8]# docker exec -it jdk8 /bin/bash
[root@29ef174768d3 usr]# 
[root@29ef174768d3 usr]# java -version
java version "1.8.0_221"
Java(TM) SE Runtime Environment (build 1.8.0_221-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.221-b11, mixed mode)
[root@29ef174768d3 usr]#   
[root@29ef174768d3 usr]# echo $JAVA_HOME 
/usr/local/java/jdk1.8.0_221
```



## 19-[理解]-Dockerfile 案例之创建镜像CentOS

> 案例：基于`CentOS:7`镜像，安装软件`vim、ifconfig`等基本配置

```ini
FROM centos:7

MAINTAINER zhangjc<zhangjc@163.com>

# 定义环境变量
ENV MYPATH /usr/local

RUN mkdir -p $MYPATH
WORKDIR $MYPATH

# 安装软件包
RUN yum -y install vim
RUN yum -y install net-tools

# 暴露端口
EXPOSE 80

CMD echo $MYPATH
CMD echo "success--------------ok"
CMD /bin/bash
```

> 执行build进行构建镜像image

```ini
[root@node1 centos7]# docker build -t mycentos:1.0 .
Sending build context to Docker daemon  2.048kB
Step 1/11 : FROM centos:7
 ---> 8652b9f0cb4c
Step 2/11 : MAINTAINER zhangjc<zhangjc@163.com>
 ---> Running in 77c86dc53dcf
Removing intermediate container 77c86dc53dcf
 ---> ef60f44d2741
Step 3/11 : ENV MYPATH /usr/local
 ---> Running in 3b3176c57df2
Removing intermediate container 3b3176c57df2
 ---> f0a8d56da47c
Step 4/11 : RUN mkdir -p $MYPATH
 ---> Running in 559b9ba6b10d
Removing intermediate container 559b9ba6b10d
 ---> 60422ea718f5
Step 5/11 : WORKDIR $MYPATH
 ---> Running in 47774d633a95
Removing intermediate container 47774d633a95
 ---> 148cd6c137ad
Step 6/11 : RUN yum -y install vim
 ---> Running in 993d28eeebd0
Loaded plugins: fastestmirror, ovl
...................................................
Removing intermediate container 993d28eeebd0
 ---> ac6c70182a26
Step 7/11 : RUN yum -y install net-tools
 ---> Running in 5a97169e3a24
Loaded plugins: fastestmirror, ovl
Loading mirror speeds from cached hostfile
Removing intermediate container 5a97169e3a24
 ---> bb497644627d
Step 8/11 : EXPOSE 80
 ---> Running in 6d4e92e6644c
Removing intermediate container 6d4e92e6644c
 ---> cf3918ea404f
Step 9/11 : CMD echo $MYPATH
 ---> Running in d9a59011f15f
Removing intermediate container d9a59011f15f
 ---> 98ba108a0939
Step 10/11 : CMD echo "success--------------ok"
 ---> Running in e21a07bb0092
Removing intermediate container e21a07bb0092
 ---> f0aa5737691c
Step 11/11 : CMD /bin/bash
 ---> Running in ebf764694d40
Removing intermediate container ebf764694d40
 ---> 3f64d0cfadf7
Successfully built 3f64d0cfadf7
Successfully tagged mycentos:1.0

[root@node1 centos7]# docker images
REPOSITORY                TAG       IMAGE ID       CREATED          SIZE
mycentos                  1.0       3f64d0cfadf7   2 minutes ago    448MB
```

> 查看构建镜像：`docker images`，基于构建镜像，创建运行容器Contanier

```ini
[root@node1 centos7]# docker run -it --name=mycentos mycentos:1.0
[root@210ed2931eae local]# ifconfig
eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 172.17.0.3  netmask 255.255.0.0  broadcast 172.17.255.255
        ether 02:42:ac:11:00:03  txqueuelen 0  (Ethernet)
        RX packets 8  bytes 656 (656.0 B)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 0  bytes 0 (0.0 B)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

lo: flags=73<UP,LOOPBACK,RUNNING>  mtu 65536
        inet 127.0.0.1  netmask 255.0.0.0
        loop  txqueuelen 1000  (Local Loopback)
        RX packets 0  bytes 0 (0.0 B)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 0  bytes 0 (0.0 B)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

[root@210ed2931eae local]# vim tt.txt
[root@210ed2931eae local]# 
[root@210ed2931eae local]# 
[root@210ed2931eae local]# pwd
/usr/local
```

> ​			上述创建交互式容器，当推出交互式命令时，容器Contanier运行终止，可以再次使用命令：`docker start`启动，[当我们start启动容器以后，属于守护式启动容器，在后台运行]()



## 20-[了解]-Docker 私有仓库之registry 搭建

> ​			在 Docker 中，当我们执行 docker pull xxx 的时候 ，它实际上是从 registry.hub.docker.com这个地址去查找，这就是Docker公司为我们提供的公共仓库。
>
> ​			在工作中，不可能把企业项目push到公有仓库进行管理。为了更好的管理镜像，Docker不仅提供了一个中央仓库，同时允许搭建本地私有仓库。
>
> [Docker中镜像存储管理——仓库Repository，类似Maven依赖管理。]()
>
> - 1）、搭建私有仓库方式一：`registry` 镜像，构建私有仓库，属于Demo简单类型
> - 2）、搭建私有仓库方式二：`harbor`构建仓库，企业中最经常使用
>
> [Docker 官方提供了一个搭建私有仓库的镜像 registry ，只需把镜像下载下来，运行容器并暴露5000端口，就可以使用了]()

```ini
[root@node1 ~]# docker pull registry:2
2: Pulling from library/registry
0a6724ff3fcd: Pull complete 
d550a247d74f: Pull complete 
1a938458ca36: Pull complete 
acd758c36fc9: Pull complete 
9af6d68b484a: Pull complete 
Digest: sha256:d5459fcb27aecc752520df4b492b08358a1912fcdfa454f7d2101d4b09991daa
Status: Downloaded newer image for registry:2
docker.io/library/registry:2

[root@node1 ~]# docker run -di -v /opt/registry:/var/lib/registry -p 5000:5000 --name=myregistry registry:2
99ef61119ebcebd08017e6b8166e1a6d36f796cc86c65655f952bad9ce0d2127


[root@node1 ~]# docker ps
CONTAINER ID   IMAGE          COMMAND                  CREATED          STATUS          PORTS                    NAMES
99ef61119ebc   registry:2     "/entrypoint.sh /etc…"   8 seconds ago    Up 5 seconds    0.0.0.0:5000->5000/tcp   myregistry


```

> ​		Registry服务默认会将上传的镜像保存在容器的/var/lib/registry，将主机的/opt/registry目录挂载到该目录，即可实现将镜像保存到主机的/opt/registry目录了。
>
> 在浏览器输入地址：http://node1.itcast.cn:5000/v2/_catalog，查看仓库中镜像有哪些。

![1612166426539](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612166426539.png)

> 通过push镜像到registry仓库中

```ini
# 1. 要通过docker tag将该镜像标志为要推送到私有仓库：
docker tag nginx:latest localhost:5000/nginx:latest

# 2. docker push 命令将 nginx 镜像 push到私有仓库中
docker push localhost:5000/nginx:latest
```

> 再次刷新私有仓库中镜像，页面截图如下：

![1612166695377](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612166695377.png)

> 当然可以使用`docker pull` 拉取本地仓库镜像。



## 21-[了解]-Docker镜像推送到阿里云镜像仓库

> 可以将本地镜像推送push到阿里云镜像仓库中，具体如下所示：
>
> [提示，需要知道阿里云账号和密码，尤其秘密，如果没有密码，无法进行登录，然后在记性pull和push操作。]()

![1612167230383](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612167230383.png)

![1612167276973](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612167276973.png)



## 22-[掌握]-Docker 部署CRM数据库MySQL

> 讲解Docker容器部署，主要由于物流项目中，业务数据存储的数据库将使用Docker容器部署。
>
> [接下来，查看项目中node1.itcast.cn虚拟机中容器，主要看一看MySQL数据库容器]()

```ini
# step1、解压node1虚拟机

# step2、导入虚拟机至VMWare，启动虚拟机
	选择我已移动虚拟机
	
# step3、使用远程命令连接虚拟机，已经安装好Docker，镜像和容器都是OK的
	友情提示：如果使用提供node1和node2虚拟机遇到环境问题时，直接删除，重新解压导入。
```

> 执行如下命令

```ini
[root@node1 ~]# systemctl status docker 
● docker.service - Docker Application Container Engine
   Loaded: loaded (/usr/lib/systemd/system/docker.service; enabled; vendor preset: disabled)
   Active: active (running) since Tue 2021-02-02 00:20:20 CST; 1min 28s ago
     Docs: https://docs.docker.com
 Main PID: 1186 (dockerd)
    Tasks: 11
   Memory: 125.6M
   CGroup: /system.slice/docker.service
           └─1186 /usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock
           
# 查看镜像
[root@node1 ~]# docker images

```

![1612167771193](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612167771193.png)

```ini
# 查看容器
[root@node1 ~]# docker ps -a
CONTAINER ID        IMAGE                             COMMAND                  CREATED             STATUS                      PORTS                                                     NAMES
28888fad98c9        canal/canal-server:v1.1.2         "/alidata/bin/main.s…"   8 months ago        Exited (255) 4 months ago   2222/tcp, 8000/tcp, 11112/tcp, 0.0.0.0:11111->11111/tcp   canal-server
8b5cd2152ed9        mysql:5.7                         "docker-entrypoint.s…"   8 months ago        Exited (255) 4 months ago   0.0.0.0:3306->3306/tcp, 33060/tcp                         mysql
cb7a41433712        kungkk/oracle11g_centos7:latest   "/bin/bash"              8 months ago        Exited (255) 4 months ago   0.0.0.0:1521->1521/tcp  
```

> 存在三个容器，启动启动MySQL数据库容器，运行MySQL数据库服务

```ini
[root@node1 ~]# docker start mysql
mysql
[root@node1 ~]# 
[root@node1 ~]# docker ps 
CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                               NAMES
8b5cd2152ed9        mysql:5.7           "docker-entrypoint.s…"   8 months ago        Up 2 seconds        0.0.0.0:3306->3306/tcp, 33060/tcp   mysql
```

![1612167937274](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612167937274.png)



> MySQL数据库用户名和密码：`root/123456`















