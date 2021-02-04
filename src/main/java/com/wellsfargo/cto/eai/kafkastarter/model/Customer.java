package com.wellsfargo.cto.eai.kafkastarter.model;

import lombok.*;

@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class Customer {

    private String id;
    private String firstName;
    private String lastName;
    private String phoneNumber;
}
