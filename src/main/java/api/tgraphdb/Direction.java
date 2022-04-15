package api.tgraphdb;

public enum Direction {
    /**
     * Defines outgoing relationships.
     */
    OUTGOING,
    /**
     * Defines incoming relationships.
     */
    INCOMING,
    /**
     * Defines both incoming and outgoing relationships.
     */
    BOTH;

    /**
     * Reverses the direction returning INCOMING if this equals
     * OUTGOING, OUTGOING if this equals INCOMING or
     * BOTH if this equals BOTH.
     *
     * @return The reversed direction.
     */
    public Direction reverse() {
        switch (this) {
            case OUTGOING:
                return INCOMING;
            case INCOMING:
                return OUTGOING;
            case BOTH:
                return BOTH;
            default:
                throw new IllegalStateException("Unknown Direction "
                        + "enum: " + this);
        }
    }
}

