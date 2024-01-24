# Thales <> BigID API

To comply with data protection laws, many companies are looking for data management platforms to help them manage user's information stored in multiple databases. One of the rights usually granted by data protection laws is the right to be forgotten, in which an individual can request companies to have their personal information removed from databases. The deletion of records from databases, though, can also remove non personal information, which can be problematic for business data analysis. To overcome this issue, our solution only anonymizes personal records (i.e. uses a cryptographic algorithm that can not be reversed), keeping non personal information intact.

The anonymization is performed by Thales Ciphertrust Token Server (CTS) and it provides this functionality through a REST API. Although BigID has support for APIs, BigID cannot process the responses from APIs and make changes in the databases. For this reason, the best workaround to utilize BigID's API support is to develop an intermediate API, which will receive the data that need to be anonymized from BigID, gather their contents and update the database with tokens.


## Supported databases
 - MySQL 8.0, 5.7 and 5.6
 - Oracle >= 12.1


## Dependencies
This API uses Flask as a framework, NGINX as a web server and is deployed in a docker container. With that said, the API requires the following dependencies:
 - Bash
 - [Docker](https://www.docker.com/)
 - [NGINX](https://www.nginx.com/)

During the installation, docker will download and install Python 3.8 and all required libraries:
 - [Flask](https://flask.palletsprojects.com/en/2.2.x/installation/)
 - [PyCryptodomex](https://pypi.org/project/pycryptodomex/)
 - [Python MySQL Connector](https://pypi.org/project/mysql-connector-python/) - Highly recommended for use with MySQL Server 8.0, 5.7, and 5.6
 - [Python Oracle Connector](https://github.com/oracle/python-oracledb) - Requires Oracle > 12.1


## API Installation and Deploy
To install, first extract the source code files and navigate to the app's root folder

```bash
$ tar -xvf thales_bigid_api_vX.X.X.tar.gz
$ cd thales_bigid_api_vX.X.X
```

Before deploying the application,:
 - Generate the Token Server's certificate;
 - Copy the Token Server's certificate to the project's root folder
 - Edit the bigid_user_token.txt file with your BigID's user token. We recommend the creation of a new user for the API in Administration Access Management. During the creation of the user, BigID will generate the user token, which can only be copied in the instance it is generated.

Also, edit the config.ini file:
 - CTS `ip`: The IP of the Token Server. Will be used with the docker build command to add the CTS to the hosts file, allowing SSL verification
 - CTS `hostname`: The hostname of the token server. If the CTS certificate is provided, the CTS hostname and the hostname in the certificate must match. To use the hostname, add the CTS to the hosts file or configure the DNS
 - CTS `certificate`: The full path to the CTS certificate
 - BigID `user_token_path`: Tha path to the bigid_user_token.txt file
 - BigID `encryption_key`: The encryption key set during BigID's installation. This key will be used to decrypt the credentials to connect to the data sources
 - DockerDeploy `host_port`: The port that will be used by the API in the host
 - DockerDeploy `docker_link_port`: The port that host_port will bind to in the docker container
 - Proxy `http`: HTTP proxy URL that will be used in requests to BigID (e.g. http://<url>:<port>)
 - Proxy `https`: HTTPS proxy URL that will be used in requests to BigID (e.g. http://<url>:<port>)

Now run the `start.sh` script to deploy the application:
```bash
$ sudo bash start.sh
```

If the deploy was successful, you will be able to see the container running and the images created:
```bash
$ docker ps
CONTAINER ID IMAGE COMMAND CREATED STATUS 
PORTS NAMES
740fdf3be13b thales_bigid.api "/entrypoint.sh /sta…" 19 hours ago Up 4 minutes 443/tcp, 0.0.0.0:5000-
>80/tcp, :::5000->80/tcp thales_bigid.api

$ docker images
REPOSITORY TAG IMAGE ID CREATED SIZE
thales_bigid.api latest c1b0dd5d4d5f 20 hours ago 1.1GB
tiangolo/uwsgi-nginx-flask python3.8 f1a807249a4e 2 days ago 942MB
```

The thales_bigid.api contains our API, while tiangolo/uwsgi-nginx-flask is an image with uWSGI and Nginx for Flask apps in Python running in a 
single container.

With the API deployed, we need to add it in BigID as a new application. To do it, go to BigID's left panel Applications Management Add App Via URL and enter the API's URL. Edit BigID Base URL with the correct BigID URL and do not type a bar after the URL. Make sure to check the box that allows the application to retrieve data source credentials, as they are necessary for our API to connect to the data sources. In the application's main page all available actions will be displayed, and we can change the parameters, run and schedule the actions.


## Usage

### Anonymization
Before running/scheduling the API, we need to have a minimization request (see the section Data Deletion/Anonymization in BigID's Usage page). All those requests will be listed in the Data Deletion application. To see these requests, go to the Left Menu Applications Management Data Deletion. Our API will search for deletion requests that are "Pending" and marked as "Delete Manually". If your request is not marked, open the request, select all objects that you want to mark and click on "Mark For Delete Manually".

Our application will be shown in the Applications Management page. From there, in our application's anonymization page, set the correct CTS and categories and save. If the categories field is empty, all fields/columns will be anonymized.

Finally, run the application. If the execution was successful, a green icon will appear besides the action name. If it was not, an error message will pop. The application logs can be downloaded in the top right menu in the Activity Logs section.

#### Note:
 - The Deletion Request needs to be "Pending" and marked as "Delete Manually" for the API to execute the anonymization
 - Even if BigID finds an individual's data in a table that has neither a primary key or the Unique Identifier, the anonymization is not performed to avoid wrong data replacements
 - When data is tokenized, it is returned as strings. Depending on the tokenization template used, the resulting token might be composed of numbers, letters and special characters, so the code will not be able to replace data in columns which the data type is not compatible with strings (e.g. integers), throwing an error and stopping the anonymization


### Remediation
 - <span style="background-color: #FFFF00">TBD</span>


## License
Thales <> BigID API is available under the <span style="background-color: #FFFF00">TBD</span> license. See the LICENSE file for more info.


## Contact
 - <span style="background-color: #FFFF00">TBD</span>