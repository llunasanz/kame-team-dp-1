
create database zurich

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
    CP int not null,
  
        
);

create table position
(
    Id_position int auto_increment primary key,
    Id_custom varchar(20) not null,
    transport string(100) null
    latitude float not null,
    longitude float not null,
    time datetime not null,
        foreign key (Id_custom) references Custom (id_custom) ON DELETE CASCADE)
  
create table friends
(
    Id_friends int auto_increment primary key,
    Id_custom varchar(20) not null,
    friend varchar(20) not null,
        foreign key (Id_custom) references Custom(Id_custom) ON DELETE CASCADE
        foreign key (friend) references Custom(customName) ON DELETE CASCADE
)
