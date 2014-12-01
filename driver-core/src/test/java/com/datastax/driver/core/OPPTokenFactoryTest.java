package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OPPTokenFactoryTest {
    Token.Factory factory = Token.OPPToken.FACTORY;

    @Test(groups = "unit")
    public void should_split_range() {
        List<Token> splits = factory.split(token('a'), token('d'), 3);
        assertThat(splits).containsExactly(
            token('b'),
            token('c')
        );
    }

    @Test(groups = "unit")
    public void should_split_range_that_wraps_around_the_ring() {
        List<Token> splits = factory.split(token('d'), token('c'), 3);
        assertThat(splits).containsExactly(
            token('a'),
            token('b')
        );
    }

    private Token token(char c) {
        return new Token.OPPToken(ByteBuffer.wrap(new byte[]{ (byte)c }));
    }
}
