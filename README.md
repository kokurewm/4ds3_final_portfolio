# 4DS3 Final Portfolio 
Part 1 application (code) showcased here.

Explanation with design specifics answered in questions 1 & 2 in portfolio document.


## Installation
```
git clone https://github.com/kokurewm/4ds3_final_portfolio
cd 4ds3_final_portfolio
pip3 install -r requirements.txt
```
### Initialization
#### From a shell
```
python3 server.py
```

#### As a service (Linux only)
```
cp 4ds3.service /etc/systemd/system
systemctl enable --now 4ds3
```
## Usage


### Adding a new job
```
# This will return the job ID.
$ client.py add -p 5 -c "sleep 100"
9ebcae3fa8
```
### Listing jobs
```
# Will return jobs sorted in ascending order by priority (in both 'running' and 'queued' seperately)

$ client.py status
{
    "running": {
        "9ebcae3fa8": {
            "command": "'sleep 100'",
            "priority": 5
        }
    },
    "queued": {
        "4b2c221f33": {
            "command": "sleep 300",
            "priority": 10
        },
        "3afa8db93b": {
            "command": "sleep 300",
            "priority": 15
        }
    }
}
```

### Deleting a running/queued job
```
$ client.py rm 9ebcae3fa8
deleted: 9ebcae3fa8
...

# Now check to make sure its gone.
$ client.py status
{
    "queued": {
        "4b2c221f33": {
            "command": "sleep 300",
            "priority": 10
        },
        "3afa8db93b": {
            "command": "sleep 300",
            "priority": 15
        }
    }
}
```
