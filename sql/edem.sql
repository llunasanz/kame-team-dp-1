create database IF NOT EXISTS zurich;
use zurich;
create table IF NOT EXISTS Custom (Id_custom varchar(20) primary key,customName varchar(100) not null,customLastName varchar(100) null,age int not null,gender varchar(20)not null,weight float null,height float null,bloodPressureSist int null,bloodPressureDiast int null,cholesterol float null,smoker bit null,drinking float null,disability bit null,previousPathology bit  null,CP int not null);
create table IF NOT EXISTS position (Id_position int auto_increment primary key,Id_custom varchar(20) not null,transport varchar(100) null,latitude float not null,longitude float not null,time datetime not null,foreign key (Id_custom) references Custom (id_custom) ON DELETE CASCADE);
create table IF NOT EXISTS friends (Id_friends int auto_incremetent primary key, Id_custom varchar(20) not null,friends_list varchar(200) null,foreign key (Id_custom) references Custom(Id_custom) ON DELETE CASCADE);
create table IF NOT EXISTS ranking (Id_ranking int auto_increment primary key, Id_custom varchar(20) ,km_walk float null,km_bike float null,punctuation float null, foreign key (Id_custom) references Custom(Id_custom) ON DELETE CASCADE);
