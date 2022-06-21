# honest-test
Hello My name is Parames Koenson
I'm not comfortable using my voice today. Instead, I use text for communication.
I built everything on aws, including RDS, EC2, MSK.
But I used aws free tier, so I had to resampling data and split files for reduce the use of resources and run time.

Let's go see my method.
1. Download and Preparing Data
2. Preparing Python (requirements,config,producer,consumer) and SQl
3. upload project to s3
4. Set Database (RDS) and build Table
5. Set MSK : I've done it. Rework takes a long time to start. So let me use the same one.
6. Set EC2 and security group

Let's test

finally,
I use the database snapshots instead of database version control because I want to focus on aws component (Database, Instance, Kafka) And I need more time to study the database version control (such as Liquibase). thk
