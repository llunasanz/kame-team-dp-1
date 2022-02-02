create database zurich;
use zurich;

create table Custom
(
    Id_custom varchar(20) primary key,
    customName varchar(100) not null,
    customLastName varchar(100) null,
    age int not null,
    gender varchar(20)not null,
    weight float null,
    height float null,
    bloodPressureSist int null,
    bloodPressureDiast int null,
    cholesterol float null,
    smoker bit null,
    drinking float null,
    disability bit null,
    previousPathology bit  null,
    CP int not null  
);

create table position
(
    Id_position int auto_increment primary key,
    Id_custom varchar(20) not null,
    transport varchar(100) null,
    latitude float not null,
    longitude float not null,
    time datetime not null,
        foreign key (Id_custom) references Custom (id_custom) ON DELETE CASCADE
);
  
create table friends
(
    Id_custom varchar(20) not null,
    friend_0 varchar(20) null,
    friend_1 varchar(20) null,
    friend_2 varchar(20) null,
    friend_3 varchar(20) null,
    friend_4 varchar(20) null,
    friend_5 varchar(20) null,
    friend_6 varchar(20) null,
    friend_7 varchar(20) null,
    friend_8 varchar(20) null,
    friend_9 varchar(20) null,
        foreign key (Id_custom) references Custom(Id_custom) ON DELETE CASCADE
);

create table ranking (
    Id_custom varchar(20) primary key,
    Km_walking float null,
    km_bike float null,
    punctuation float null, 
	foreign key (Id_custom)references Custom(Id_custom) ON DELETE CASCADE
);