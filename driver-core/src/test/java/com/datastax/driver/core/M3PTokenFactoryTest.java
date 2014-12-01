package com.datastax.driver.core;

import java.util.List;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class M3PTokenFactoryTest {
    Token.Factory factory = Token.M3PToken.FACTORY;

    @Test(groups = "unit")
    public void should_split_range() {
        List<Token> splits = factory.split(factory.fromString("-9223372036854775808"), factory.fromString("4611686018427387904"), 3);
        assertThat(splits).containsExactly(
            factory.fromString("-4611686018427387904"),
            factory.fromString("0")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_that_wraps_around_the_ring() {
        List<Token> splits = factory.split(factory.fromString("4611686018427387904"), factory.fromString("0"), 3);
        assertThat(splits).containsExactly(
            factory.fromString("-9223372036854775808"),
            factory.fromString("-4611686018427387904")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_when_division_not_integral() {
        List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("11"), 3);
        assertThat(splits).containsExactly(
            factory.fromString("4"),
            factory.fromString("8")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_producing_empty_splits() {
        List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("2"), 5);
        assertThat(splits).containsExactly(
            factory.fromString("1"),
            factory.fromString("2"),
            factory.fromString("2"),
            factory.fromString("2")
        );
    }
}