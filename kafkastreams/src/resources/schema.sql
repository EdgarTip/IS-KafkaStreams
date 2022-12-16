DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS mainTable;


CREATE TABLE locations (
    id     SERIAL,
    city     VARCHAR(512) NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE mainTable (
    id     SERIAL,
    weatherStation VARCHAR(512) NOT NULL,
    temperature FLOAT not NULL, 
    PRIMARY KEY(id)
);

