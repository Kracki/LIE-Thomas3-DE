# Kubernetes 101

- Type of Challenge: `learning`
- Duration: `2 days`
- Deadline: `12/01/2024`
- Participant(s): `solo`

## Learning objectives

- Be able to understand what is Kubernetes
- Be able to understand the Kubernetes' basic vocabulary
- Be able to use the Kubernetes' basic commands
- Be able to create a Kubernetes cluster
- Be able to deploy an app
- Be able to explore your app
- Be able to expose your app publicly
- Be able to scale up your app
- Be able to update your app


## Introduction

Kubernetes (K8s) is simply a container orchestration framework. It means that K8s is a system designed to automate the lifecycle of containerized applications — from predictability, and scalability to availability.

The reason behind the rise and need for K8s stems from the increasing use of microservices, away from traditional monolithic-type applications. As a result, containers provide the perfect host for these individual microservices as containers manage dependencies, are independent, OS-agnostic, and ephemeral, amongst other benefits.

Complex applications that have many components are often made up of hundreds or even thousands of microservices. Scaling these microservices up while ensuring availability is an extremely painful process if we were to manage all these different components using custom-written programs or scripts, resulting in the demand for a proper way of managing these components. This is where container orchestration tools like K8s come into play.

## The mission

Today your mission will be to understand the core concepts of Kubernetes, the architecture of the system, the problems it solves, and the model that it uses to handle containerized deployments and scaling.

After diving into the theory, you should be able to explain the architecture of a k8s cluster, and list different constituents of a master node and their respective roles (API Server, Scheduler, Controller Manager, etc). As well as what makes a worker node a worker node (Kublet, container runtime, Kube-proxy,...). After focusing on the architecture, you will now focus on the different components of a k8s system, such as pods, services, deployments, ingress, ingress controllers, ConfigMaps, Secrets, Persistent Volumes, Persistent Volumes Claims, DeamonSets, ReplicaSets, StatefullSets, etc...

Remember that you should be able to explain what each of these components does as if you are in an interview for a job.

Also, make sure you do not forget to learn about the tooling (kubectl, kubeadm, minikube, etc...)

Finally, get your hands dirty with [the 6 interactive labs on the basic k8s modules](https://kubernetes.io/docs/tutorials/kubernetes-basics/).

**Complete the 6 modules!**

*Hand-on practice is the best way to learn this kind of tool!*

## Want more hands-on?

- [K8s stateful application](https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/)
- [K8s with Cassandra DB](https://kubernetes.io/docs/tutorials/stateful-application/cassandra/)


## Complementary Resources

- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Kube Academy](https://kube.academy/)
- [Pluralsight - Getting Started with Kubernetes](https://app.pluralsight.com/library/courses/kubernetes-getting-started/table-of-contents)
- [Medium - 8 Best Free Kubernetes Courses for Beginners in 2022](https://medium.com/javarevisited/7-free-online-courses-to-learn-kubernetes-in-2020-3b8a68ec7abc)
- [Edx - Introduction to Kubernetes](https://www.edx.org/course/introduction-to-kubernetes?source=aw&awc=6798_1660107853_6f3cd051bc758e5aca33cb66504e75cb&utm_source=aw&utm_medium=affiliate_partner&utm_content=text-link&utm_term=631878_javarevisited)

![Boats shiping boats](https://starecat.com/content/wp-content/uploads/this-is-a-ship-shipping-ship-shipping-shipping-ships.jpg)
