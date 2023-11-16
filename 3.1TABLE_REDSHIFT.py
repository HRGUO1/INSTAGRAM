DROP TABLE brand;
CREATE TABLE brand (
    Brand_name varchar(100),
    Brand_id BIGINT PRIMARY KEY
)
DISTSTYLE EVEN;

DROP TABLE account;
CREATE TABLE account (
    Account_id BIGINT PRIMARY KEY, 
    Account_name VARCHAR(300),     
    Account_followersCount INT,     
    Account_followsCount INT,       
    Account_postsCount INT,         
    Account_url VARCHAR(600)       
)
DISTSTYLE EVEN;


DROP TABLE partnership_account;
CREATE TABLE partnership_account (
    Post_username VARCHAR(300),  
    Post_owner_id BIGINT PRIMARY KEY,    
    Post_brand VARCHAR(300)        
)
DISTSTYLE EVEN;

DROP TABLE post;
CREATE TABLE post (
    Post_id BIGINT PRIMARY KEY,
    Post_username VARCHAR(300),
    Post_ownerId BIGINT,
    Post_comments INTEGER,
    Post_likes INTEGER,
    Post_timestamp DATE,
    Post_url VARCHAR(500)
)
DISTSTYLE EVEN;

DROP TABLE master;
CREATE TABLE master (
    Post_id BIGINT NOT NULL PRIMARY KEY,
    Post_username VARCHAR(255),
    Post_url VARCHAR(300),
    Post_comments INT,
    Post_likes INT,
    Post_timestamp DATE,
    Post_ownerId BIGINT,
    Post_type VARCHAR(50),
    Post_videoView DOUBLE PRECISION,
    Post_videoPlay DOUBLE PRECISION,
    Post_brand VARCHAR(255)
);
