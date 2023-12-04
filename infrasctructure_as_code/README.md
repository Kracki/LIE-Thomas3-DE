# Infrastructure as Code (IaC)

![Infrastucture as Code](https://learn.microsoft.com/en-us/devops/_img/infrastructureascode_600x300-3.png)

Infrastructure as Code (IaC) is a way of managing your infrastructure in a version-controlled way. This means that you can use the same tools and processes that you use for your application code to manage your infrastructure. This is a very powerful concept and allows you to treat your infrastructure as a first-class citizen in your development process.

## The problem
When using the cloud, you will face complicated web interfaces with a lot of options. This makes it hard to keep track of what is deployed and how. It also makes it hard to automate the deployment of your infrastructure. Imagine if you have to deploy 100 servers with the same option. You would have to click through 100 times to set the same options. You also risk forgetting to click this hidden box multiple times and end up with a non-standard deployment. Then, what if you need to explain to a new team member how to deploy your infrastructure? You would have to explain all the options and how to set them. This is a lot of work and is error-prone.


## The solution
One solution could be to use AWS CLI. It would be easier to automate deployment and make it more consistent. However, it is still hard to keep track of what is deployed and how. It is also hard to share the deployment process with other team members. You would have to write a lot of documentation and explain how to use the CLI. Different versions depend on the operating system. You would need to learn Bash in depth. Definitely not the best solution.

This is where IaC comes in. You can use a tool like Terraform to define your infrastructure in a declarative way. This means that you can define your infrastructure in a file and then let Terraform do the work for you. This is a lot faster and more reliable than doing it manually.

There are a lot of tools that can be used for IaC. Each cloud provider has its own.
- AWS: CloudFormation
- Azure: ARM Templates
- Google Cloud: Deployment Manager

They all have their specificities. For example, CloudFormation is very powerful but also very verbose. ARM Templates are very concise but also very limited.

It would be a lot of work to learn all of them. That's why we will use Terraform. It is a very powerful tool that can be used with all major cloud providers. It is also very concise and easy to learn.

### Terraform


<img src="https://www.vectorlogo.zone/logos/terraformio/terraformio-ar21.png" alt="data storage" height="200"/>


Terraform is a tool that allows you to define your infrastructure in a declarative way. It is a very powerful tool that allows you to define your infrastructure in a way that is very similar to how you would define your application code. You can then, version your deployment processes with git, share it, and redeploy your whole infrastructure in a second,...

### Hashicorp


<img src="https://www.vectorlogo.zone/logos/hashicorp/hashicorp-ar21.png" alt="data storage" height="200"/>


Terraform is a product of Hashicorp. Hashicorp is a company that develops a lot of tools for infrastructure management. They are the creators of Terraform, Vagrant, Packer, Vault, Consul, Nomad, and a lot more. They are also the creators of the Hashicorp Configuration Language (HCL). HCL is a language that is used by Terraform, Packer, and Vagrant. It is a very powerful language that allows you to define your infrastructure in a very concise way.


## Comparision

To understand the power of IaC, let's compare it to the manual way of deploying your infrastructure.

For the example, we will see how to deploy an EC2 (Simple Linux machine) on AWS.

### Web UI
If you want to do your deployment using the AWS *(to take an example)* web UI, it would look something like this blog post.

**Do not read the whole article, it is very long. Just look at all the steps, and think about what you would have to do to do it 100 times.**

- [Part 1](https://medium.datadriveninvestor.com/how-to-deploy-app-on-amazon-web-service-aws-ec2-instance-part-1-d6671867848d)
- [Part 2](https://medium.datadriveninvestor.com/how-to-deploy-app-on-amazon-web-service-aws-ec2-instance-part-2-88c1e92820c2)

### AWS CLI
If you want to do your deployment using the AWS CLI, it would look something like this blog post.

- [CLI Blog post](https://medium.com/@corymaklin/tutorial-amazon-web-services-part-1-create-virtual-machines-with-aws-cli-b900702bf286)

### Terraform

If you want to do your deployment using Terraform, it would look something like this:

A file that describes your infrastructure:

```hcl
resource "aws_key_pair" "admin" {
    key_name    =   "admin"
    public_key  =   ""
}

resource "aws_instance" "server1" {
    ami             = "ami-045fa58af83eb0ff4"
    instance_type   = "t2.micro"
    key_name        = "admin"
 } 
provider "aws" {
    profile =   "default"
    region  =   "eu-west-3"
}

resource "aws_default_security_group" "default" {
    vpc_id = "${aws_default_vpc.default.id}"
    ingress {
        # TLS (change to whatever ports you need)
        from_port = 22
        to_port = 22
        protocol = "tcp"
        # Please restrict your ingress to only necessary IPs and ports.
        # Opening to 0.0.0.0/0 can lead to security vulnerabilities.
        cidr_blocks = ["0.0.0.0/0"]
    }
    egress {
        from_port = 0
        to_port = 0
        protocol = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
 } 

resource "aws_default_vpc" "default" {
    tags = {
        Name = "Default VPC"
    }
}
```

And then, you can run the following command to deploy your infrastructure:

```bash
terraform apply
```

## Conclusion

As you can see, IaC is a very powerful concept. It's now a golden standard in the industry and if you are working with any cloud provider you will face that at one moment or another.

## Next steps
[Practice time](./1.Practice.md)