+++
date = "2015-03-17T15:36:56Z"
title = "Installation"
[menu.main]
  parent = "MongoDB Async Driver"
  identifier = "Async Installation"
  weight = 1
  pre = "<i class='fa'></i>"
+++

# Installation


The recommended way to get started using one of the drivers in your project is with a dependency management system.

{{< distroPicker >}}

## MongoDB Async Driver

The MongoDB Async Driver provides asynchronous API that can leverage either Netty or AsynchronousSocketChannel for fast and non-blocking I/O.

{{< install artifactId="mongodb-driver-async" version="3.11.3" dependencies="true">}}
