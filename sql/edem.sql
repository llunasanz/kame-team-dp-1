
create database zurich

create table Custom
(
    Id_custom varchar(20) primary key,
    customName varchar(100) not null,
    customLastName varchar(100) null,
    age int not null,
    gender varchar(20) null,
    weight float  null,
    height float null,
    bloodPressureSist int not null,
    bloodPressureDiast int not null,
    cholesterol float null,
    smoker bit default null,
    drinking float  null,
    disability bit default null,
    previousPathology bit default null,
    CP int not null,

);

create table position
(
    Id_position int auto_increment primary key,
    Id_custom varchar(20) not null,
    transport varchar(100) null
    latitude float not null,
    longitude float not null,
    time datetime not null,
        foreign key (clientsId) references Custom (id_custom))
  
create table friends
(
    Id_position int auto_increment primary key,
    Id_custom varchar(20) not null,
    friends varchar(20) not null,
        foreign key (Id_custom) references Custom(Id_custom)
        foreign key (friends) references Custom(Id_custom)
)