package org.springframework.batch.item.elasticsearch;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TestMigrationDomain {

    private String name;

    private int age;

    private LocalDateTime created;

    public TestMigrationDomain(TestDomain testDomain) {
        this.name = testDomain.getName();
        this.age = testDomain.getAge();
        this.created = testDomain.getCreated();
    }

}
