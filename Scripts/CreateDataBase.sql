CREATE DATABASE Locadora;
/*==============================================================*/
/* Table: ALUGUEL                                               */
/*==============================================================*/
create table ALUGUEL (
   ID                   NUMERIC              not null,
   DATA_ALUGUEL         TIMESTAMP            not null,
   DATA_DEVOLUCAO       TIMESTAMP            null,
   CLIENTE_ID           NUMERIC              not null,
   OPERADOR_ID          NUMERIC              not null,
   VALOR                DOUBLE PRECISION     null,
   constraint PK_ALUGUEL primary key (ID)
);

/*==============================================================*/
/* Table: ALUGUEL_FILME                                         */
/*==============================================================*/
create table ALUGUEL_FILME (
   ALUGUEL_ID           NUMERIC              not null,
   FILME_ID             NUMERIC              not null,
   constraint PK_ALUGUEL_FILME primary key (ALUGUEL_ID, FILME_ID)
);

/*==============================================================*/
/* Table: ATOR                                                  */
/*==============================================================*/
create table ATOR (
   ID                   NUMERIC              not null,
   NOME                 VARCHAR(255)         not null,
   constraint PK_ATOR primary key (ID)
);

/*==============================================================*/
/* Table: CLIENTE                                               */
/*==============================================================*/
create table CLIENTE (
   ID                   NUMERIC              not null,
   NOME                 VARCHAR(255)         not null,
   CPF                  VARCHAR(11)          not null,
   IDADE                INT4                 not null,
   constraint PK_CLIENTE primary key (ID)
);

/*==============================================================*/
/* Table: FILME                                                 */
/*==============================================================*/
create table FILME (
   ID                   NUMERIC              not null,
   TITULO               VARCHAR(255)         not null,
   GENERO_ENUM          INT2                 not null,
   CLASSIFICACAO        INT4                 null,
   PRECO                DOUBLE PRECISION     not null,
   constraint PK_FILME primary key (ID)
);

/*==============================================================*/
/* Table: FILME_ATOR                                            */
/*==============================================================*/
create table FILME_ATOR (
   ATOR_ID              NUMERIC              not null,
   FILME_ID             NUMERIC              not null,
   constraint PK_FILME_ATOR primary key (ATOR_ID, FILME_ID)
);

/*==============================================================*/
/* Table: USUARIO                                               */
/*==============================================================*/
create table USUARIO (
   ID                   NUMERIC              not null,
   NOME                 VARCHAR(255)         not null,
   LOGIN                VARCHAR(100)         not null,
   SENHA                VARCHAR(50)          not null,
   constraint PK_USUARIO primary key (ID)
);

alter table ALUGUEL
   add constraint FK_ALUGUEL_CLIENTE foreign key (CLIENTE_ID)
      references CLIENTE (ID)
      on delete restrict on update restrict;

alter table ALUGUEL
   add constraint FK_ALUGUEL_OPERADOR foreign key (OPERADOR_ID)
      references USUARIO (ID)
      on delete restrict on update restrict;

alter table ALUGUEL_FILME
   add constraint FK_ALUGUEL_FILME_ALUGUEL foreign key (ALUGUEL_ID)
      references ALUGUEL (ID)
      on delete restrict on update restrict;

alter table ALUGUEL_FILME
   add constraint FK_ALUGUEL_FILME_FILME foreign key (FILME_ID)
      references FILME (ID)
      on delete restrict on update restrict;

alter table FILME_ATOR
   add constraint FK_FILME_ATOR_ATOR foreign key (ATOR_ID)
      references ATOR (ID)
      on delete restrict on update restrict;

alter table FILME_ATOR
   add constraint FK_FILME_ATOR_FILME foreign key (FILME_ID)
      references FILME (ID)
      on delete restrict on update restrict;

