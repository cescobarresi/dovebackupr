#! /usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set syntax=python
#
# Author: Francesco Barresi <cesco.barresi@gmail.com>
#
"""
Dovebackupr

Usage:
    dovebackupr run [options]
    dovebackupr once [options]
    dovebackupr (-h | --help)
    dovebackupr --version

Commands:
    run             Run backups forever
    once            Run backups once and exit

Options:
    -c --config file        Config file. [default: dovebackupr.conf]
    --version
    -h --help               Show this screen.

"""

import logging, logging.handlers, collections, codecs, os, sys, time, signal, subprocess
from fasteners import InterProcessLock
from datetime import datetime
import toml, docopt, setproctitle
from .__version__ import __version__

running_backups = []

def load_configuration(config_path, default_config={}):
    def deep_update_dict(d, u):
        """Update deep dictionary"""
        for k, v in u.items():
            if isinstance(v, collections.Mapping):
                r = deep_update_dict(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        return d

    try:
        user_config = toml.load(config_path)
    except IOError:
        print("ERROR: I/O fatal error.", file = sys.stderr)
        raise SystemExit(-1)
    except toml.TomlDecodeError as tomlExp:
        for string in tomlExp.args:
            print("ERROR: Invalid TOML syntax. " + string, file = sys.stderr)
        raise SystemExit(-1)
    except TypeError as typeExp:
        for string in typeExp.args:
            print("ERROR: Invalid config file. " + string, file = sys.stderr)
        raise SystemExit(-1)
    except:
        print("ERROR: Invalid config file. Please make sure it is UTF-8 encoded and complies TOML specification.", file = sys.stderr)
        print("Please review TOML specification at: https://github.com/toml-lang/toml", file = sys.stderr)
        raise SystemExit(-1)

    return deep_update_dict(default_config, user_config)

def spawn_mailbox_backup(mailbox):
    logger = logging.getLogger('dovebackupr')
    cmd = ['doveadm', '-c', mailbox['config_file'], 'backup', '-R', '-u', mailbox['local_user'], "imapc:"]

    doveadm_backup = subprocess.Popen(cmd, stderr=subprocess.PIPE, universal_newlines=True)
    logger.debug("spawn_mailbox_backup: run backup for \"%s\"" % mailbox['name'])
    stdout, stderr = doveadm_backup.communicate()

    return {
        'since':datetime.now(),
        'last_timeout_warning':datetime.now(),
        'subprocess':doveadm_backup, 
        'error':stderr[-200:], # last 200 bytes of error stream
        'name':mailbox['name']
    }

def start_logger(config):
    logger = logging.getLogger('dovebackupr')
    if config['main']['log_level'].strip() == "debug":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    # create a file handler
    fh = logging.handlers.RotatingFileHandler(
            config['main']['log_file'],
            maxBytes=10485760,
            backupCount=10,
            )
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handler to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

def generate_config_files(all_mailbox, base_config_dir):
    logger = logging.getLogger('dovebackupr')
    dovecot_config = subprocess.run(['doveadm','config','-nP'], stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True)
    if dovecot_config.returncode != 0:
        logger.error("Error while running doveadm config -nP: %s" % ( dovecot_config.returncode) )
        raise SystemExit(4)
    base_config = dovecot_config.stdout
    for mailbox in all_mailbox:
        mailbox_config_filepath = os.path.join(base_config_dir, mailbox['name'])
        logger.info("Building configuration file '%s'" % mailbox_config_filepath)
        with open(mailbox_config_filepath, mode="w", encoding="utf8") as new_conf:
            new_conf.writelines(base_config)
            new_conf.write('imapc_host=%s\n' % mailbox['server'])
            new_conf.write('imapc_user=%s\n' % mailbox['username'])
            new_conf.write('imapc_password=%s\n' % mailbox['password'])
            new_conf.write('mail_fsync=never\n')
            new_conf.write('imapc_features=rfc822.size fetch-headers\n')
            new_conf.write('imapc_port=%s\n' % mailbox['port'])
            new_conf.write('imapc_ssl=%s\n' % mailbox['imap_ssl'])
            new_conf.write('ssl_client_ca_dir=/etc/ssl\n')
            new_conf.write('imapc_ssl_verify=%s\n' %mailbox['ssl_verify'])
        os.chmod(mailbox_config_filepath, 0o640)
        mailbox['config_file'] = mailbox_config_filepath

def get_index_by_name(all_mailbox, name):
    """Return index by name"""
    for i,m in enumerate(all_mailbox):
        if m['name'] == name:
            return i
    return None

def main_loop(all_mailbox, run_forever, config):
    logger = logging.getLogger('dovebackupr')
     # Main loop
    logger.info("Starting main loop")

    maxspawn=config['main']['max_spawn']
    minwait=config['main']['min_wait']
    if maxspawn <= 0:
        logger.error("main_loop: maxspawn must be a positive integer. It is set to \"%s\"." % maxspawn)
        raise SystemExit(3)

    all_mailbox = list(all_mailbox.values())
    all_mailbox_count = len(all_mailbox)
    # Create config file for each mailbox
    generate_config_files(all_mailbox, config['main']['mailboxes_config_dir'])

    last_run_index = -1
    global running_backups
    running_backups = []
    retry_backups = []
    backups_with_error = []

    while True:
        # Check if any backup finished
        for i, running in enumerate(running_backups):
            return_code = running['subprocess'].poll()
            if return_code is not None:
                logger.debug("main_loop: subprocess for '%s' finished with return code '%s'" %(running['name'], return_code))
                #running['error'] = running['subprocess'].stderr.read()
                if return_code == 0:
                    # Successful backup
                    logger.info("backup of '%s' finished in '%s'" % (running['name'], datetime.now() - running['since'] ))
                    # Clear from retry_backup, if it is there
                    if running['name'] in retry_backups:
                        del retry_backups[retry_backups.index(running['name'])]
                elif 'reconnecting' in running['error']:
                    # dsync error 75: timed out after 30 seconds - reconnecting
                    logger.info("Error (%s) while running backup for %s: '%s' " % (
                        return_code, running['name'], running['error']) )
                elif 'unable to connect to server' in running['error'] and running['name'] not in retry_backups:
                    # set for retry
                    retry_backups.append(running['name'])
                    logger.info("Error (%s) while running backup for %s: '%s' " % (
                        return_code, running['name'], running['error']) )
                    logger.info("Set for retry '%s'" % running['name'])
                else:
                    # Not handled error
                    logger.error("Error (%s) while running backup for %s: '%s' " % (
                        return_code, running['name'], running['error']) )
                    # Clear from retry_backup, if it is there
                    if running['name'] in retry_backups:
                        del retry_backups[retry_backups.index(running['name'])]
                    backups_with_error.append(running)
                # Clear 
                del running_backups[i]
                # Set last run timestamp
                all_mailbox[get_index_by_name(all_mailbox,running['name'])]['last_run'] = datetime.now()
            # Chek for kill_timeout
            elif (datetime.now() - running['since']).seconds >= (60*int(config['main']['kill_timeout'])):
                logger.warning("backup of '%s' it has been running since '%s', it will now be killed because it reached the kill_timeout" % (
                    running['name'], datetime.now() - running['since'] ))
                running['subprocess'].kill()
                all_mailbox[get_index_by_name(all_mailbox,running['name'])]['last_run'] = datetime.now()
            # warn of still running backups every warning_timeout minutes
            elif (datetime.now() - running['last_timeout_warning']).seconds >= (60*int(config['main']['warning_timeout'])):
                running['last_timeout_warning'] = datetime.now()
                logger.warning("backup of '%s' it has been running for '%s'" % (running['name'], datetime.now() - running['since'] ))


        logger.debug("main_loop: Running backups: %s" % len(running_backups))

        # Restart backups set for retry
        for backup_name in retry_backups:
            retry_run = get_index_by_name(all_mailbox, backup_name)
            if get_index_by_name(running_backups, backup_name):
                # Backup already running, skip
                continue
            if not retry_run:
                logger.error("Something weird happend, can't find '%s' in all_mailbox. Can't retry backup." % backup_name)
                continue
            retry_run = all_mailbox[retry_run]
            logger.info("spawning new 'retry' backup process for '%s'" % retry_run['name'])
            spawn_backup = spawn_mailbox_backup(retry_run)
            running_backups.append(spawn_backup)

        # All backuped up and not running forever? Break
        if len(running_backups) == 0 and not run_forever and last_run_index == all_mailbox_count - 1:
            # Report backups with errors
            for backup in backups_with_error:
                logger.info("Backup of '%s' returned with error: '%s'" % (backup['name'], backup['error']))
            logger.info("All backups run, exiting main loop")
            break

        # Spawn new subprocess
        while True:
            # Calculate free slots to spawn a new subprocess 
            free_slots = maxspawn - len(running_backups)
            if free_slots <= 0:
                if free_slots < 0:
                    logger.error("free_slots is less than zero, something it's wrong. running_backups = %s" % len(running_backups) )
                break

            # Select next mailbox to backup
            this_run_index = last_run_index + 1
            if this_run_index == all_mailbox_count and run_forever:
                # wrap around
                last_run_index = -1
                break
            elif this_run_index == all_mailbox_count:
                # end of list
                break

            this_run = all_mailbox[this_run_index]
            already_running = False
            # Skip mailbox with already running backups
            for running in running_backups:
                if running['name'] == this_run['name']:
                    already_running = True
                    break
            if already_running and all_mailbox_count == 1:
                logger.debug("main_looop: finished looking for next mailbox to backup")
                break # nothing to backup for now
            elif already_running:
                last_run_index += 1
                continue # skipt to next

            # Check minimal wait between backups
            if 'last_run' in this_run and (datetime.now() - this_run['last_run']).seconds < minwait:
                last_run_index += 1
                continue # skipt to next

            # Spawn new backup process
            logger.info("spawning new backup process for '%s'" % this_run['name'])
            spawn_backup = spawn_mailbox_backup(this_run)
            running_backups.append(spawn_backup)
            last_run_index += 1
            logger.debug("last_run_index: %s" % last_run_index)

        if len(running_backups) != 0:
            # Ther is no need to loop at max speed because the backup of a mailbox
            # will take more than miliseconds.
            time.sleep(5)

def handle_sigint(signum, frame):
    """Sends SIGKILL to subprocesses and exites"""
    logger = logging.getLogger('dovebackupr')
    logger.info("Received: SIGINT")

    # Send SIGKILL to all subprocesses
    for running in running_backups:
        logger.info("Killing backup process for '%s'" % running['name'])
        running['subprocess'].kill()
    logger.info("Exiting")
    raise SystemExit(0)

def handle_sigterm(signum, frame):
    """Sends SIGTERM to subprocesses and waits for all to exit."""
    global running_backups
    logger = logging.getLogger('dovebackupr')
    logger.info("Received: SIGTERM")

    # Send SIGTERM to all subprocesses
    for running in running_backups:
        logger.info("Sent SIGTERM to backup process for '%s'" % running['name'])
        running['subprocess'].terminate()

    logger.info("Waiting for processes to exit")

    # Wait for subprocesses to exit
    while running_backups:
        terminated = []
        for i, running in enumerate(running_backups):
            running['subprocess'].poll()
            if running['subprocess'].returncode is not None:
                logger.info("Backup process for '%s' terminated" % running['name'])
                terminated.append(i)
        # remove items backward so that indexes are valid
        terminated.reverse()
        for i in terminated:
            del running_backups[i]
        # wait for it
        time.sleep(1)

    logger.info("Al backups are terminated.")
    logger.info("Exiting")
    raise SystemExit(0)

def drop_privileges(user, group):
    """
    Set privileges to given user and group
    """
    import pwd, grp

    # Get the uid/gid from the name
    uid = pwd.getpwnam(user).pw_uid
    gid = grp.getgrnam(group).gr_gid

    # Remove group privileges
    os.setgroups([])

    # Try setting the new uid/gid
    os.setgid(gid)
    os.setuid(uid)

def main():
    arguments = docopt.docopt(__doc__, version=__version__)

    # Load configuration
    default_config = {
        'main':{
            'warning_timeout':15,
            'kill_timeout':30,
            'max_spawn':4,
            'min_wait':15*60,
            'log_level':'info',
            'runas_user':None,
            'runas_group':None,
            }
        }
    config = load_configuration(arguments['--config'], default_config)

    # Set procname
    try:
        setproctitle.setproctitle('dovebackupr %s' % " ".join(sys.argv[1:]) )
    except Exception as e:
        print("Cant set process title: %s" % e)

    # Drop privileges
    if config['main']['runas_user'] is not None and config['main']['runas_group'] is not None:
        drop_privileges(config['main']['runas_user'], config['main']['runas_group'])

    # Setup logger
    logger = start_logger(config)

    # Acquire lock to prevent multiple instances running
    lock = InterProcessLock(config['main']['lock_file'])
    acquired_lock = lock.acquire(blocking=False)
    if not acquired_lock:
        print("dovebackupr is already running.\nlock file: '%s'\nExiting now." % config['main']['lock_file'])
        logger.error("dovebackupr is already running.\nlock file: '%s'\nExiting now." % config['main']['lock_file'])
        raise SystemExit(2)

    mailboxes = config['mailbox']
    if arguments.get('single',None):
        try:
            mailbox = mailboxes[arguments['<mailbox>']]
        except KeyError:
            logger.error("Given mailbox '%s' not found in configuration file '%s'" % (arguments['<mailbox>'], arguments['--config']))
            raise SystemExit(-1)
        mailboxes = {arguments['<mailbox>']:mailbox}

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigterm)
    logger.info("dovebackupr started. Logging at '%s', with debug=%r" % (config['main']['log_file'], config['main']['log_level']=='debug'))
    # Run main loop
    main_loop(mailboxes, run_forever=arguments['run'], config=config)

if __name__ == '__main__':
    main()
