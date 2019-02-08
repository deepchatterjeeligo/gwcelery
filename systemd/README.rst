Manage GWCelery processes with systemd
======================================

This directory provides support for starting, stopping, and supervising
GWCelery using systemd_, the service manager for Linux. Systemd is present on
many Linux systems and is responsible for bootstrapping userspace and bringing
up system services. The main advantage of using systemd instead of the existing
htcondor technique is systemd can stop and restart the components of GWCelery
in the proper sequence to minimize downtime and reduce the chances of losing
in-flight tasks.

There is also an instance of systemd for each user that can manage that user's
services. This example uses the per-user systemd instance. The following
instructions were tested on Debian buster.

1.  Check out this branch and install the code with pip as you normally would::

        $ git clone https://git.ligo.org/leo-singer/gwcelery ~/src
        $ cd ~/src
        $ git checkout --track origin/systemd
        $ pip3 install --user .

    As usual, also make sure that you have provided authentication information
    for GraceDb by running ``ligo-proxy-init`` and for LVAlert by completing
    your ``~/.netrc`` file.

2.  The command above installed the script ``gwcelery`` into ``~/.local/bin``.
    Create an environment variable file to add it to the PATH for systemd::

        $ mkdir -p ~/.config/environment.d
        $ cd ~/.config/environment.d
        $ echo 'PATH=$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin' > 10-path.conf

    If you want to use a non-default GWCelery configuration, then this is a
    good time to add the ``CELERY_CONFIG_MODULE`` environment variable to a
    file in ``~/.config/environment.d``.

3.  Create a symbolic link to add the directory that contains this README file
    to the user systemd unit search path::

        $ mkdir -p ~/.local/share/systemd
        $ cd ~/.local/share/systemd
        $ ln -s ../../../src/systemd user

4.  Tell systemd to reload configuration files::

        $ systemctl --user daemon-reload

5.  Tell systemd to bring up GWCelery::

        $ systemctl --user start gwcelery

6.  You can now manage GWCelery like any other system service. Here are some
    example commands to try::

        $ systemctl --user status gwcelery.target
        $ systemctl --user stop gwcelery.target
        $ systemctl --user restart gwcelery.target

.. _systemd: https://freedesktop.org/wiki/Software/systemd/
