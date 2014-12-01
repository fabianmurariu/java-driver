package com.datastax.driver.core;

import java.util.List;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RPTokenFactoryTest {
    Token.Factory factory = Token.RPToken.FACTORY;

    @Test(groups = "unit")
    public void should_split_range() {
        List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("127605887595351923798765477786913079296"), 3);
        assertThat(splits).containsExactly(
            factory.fromString("42535295865117307932921825928971026432"),
            factory.fromString("85070591730234615865843651857942052864")
        );
    }

    @Test(groups = "unit")
    public void should_split_range_that_wraps_around_the_ring() {
        List<Token> splits = factory.split(
            factory.fromString("127605887595351923798765477786913079296"),
            factory.fromString("85070591730234615865843651857942052864"),
            3);

        assertThat(splits).containsExactly(
            factory.fromString("0"),
            factory.fromString("42535295865117307932921825928971026432")
        );
    }
}
