-- auto-generated definition
create table payment
(
    id             int auto_increment
        primary key,
    user_id        int                                                                          null,
    amount         decimal(12, 2)                                                               not null,
    payment_method varchar(50)                                                                  not null,
    payment_status enum ('pending', 'success', 'failed', 'cancelled') default 'pending'         null,
    transaction_id varchar(100)                                                                 null,
    description    text                                                                         null,
    created_at     timestamp                                          default CURRENT_TIMESTAMP null,
    updated_at     timestamp                                          default CURRENT_TIMESTAMP null on update CURRENT_TIMESTAMP,
    constraint transaction_id
        unique (transaction_id)
);