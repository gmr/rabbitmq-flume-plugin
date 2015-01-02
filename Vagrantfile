# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT

echo "Configuring apt repositories"
# Add the external repositories
add-apt-repository -y ppa:webupd8team/java
echo "deb http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib" > /etc/apt/sources.list.d/cloudera.list
echo "deb http://packages.erlang-solutions.com/debian precise contrib" > /etc/apt/sources.list.d/erlang-solutions.list
echo "deb http://www.rabbitmq.com/debian/ testing main" > /etc/apt/sources.list.d/rabbitmq.list

# Add the various external repositories
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv F7B8CEA6056E8E56
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv D208507CA14F4FCA
curl -s http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key | apt-key add -

# Tell Java Install license is accepted
echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections

# Update the repository indexes and install Java, Erlang, and RabbitMQ
apt-get -q update
apt-get install -y rabbitmq-server oracle-java7-installer flume-ng
apt-get -q clean

# Enable the RabbitMQ management interface
rabbitmq-plugins enable rabbitmq_management

# Stop services that are not needed
service chef-client stop
service puppet stop
service cron stop
service atd stop

# Copy the flume test configs over
 cp /vagrant/conf/flume* /etc/flume-ng/conf/
SCRIPT

Vagrant.configure('2') do |config|
  config.vm.hostname = 'flume'
  config.vm.box = 'ubuntu-trusty'
  config.vm.box_url = 'https://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box'

  # config.vbguest.auto_update = false

  config.vm.provision "shell", inline: $script

  config.vm.provider "virtualbox" do |v|
    v.memory = 4098
    v.cpus = 2
  end

  config.vm.network :forwarded_port, guest: 80, host: 8000
  config.vm.network :forwarded_port, guest: 3141, host: 3141
  config.vm.network :forwarded_port, guest: 5672, host: 5672
  config.vm.network :forwarded_port, guest: 15_672, host: 15_672
end
