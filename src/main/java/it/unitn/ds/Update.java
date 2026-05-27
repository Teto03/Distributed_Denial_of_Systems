package it.unitn.ds;

import java.io.Serializable;
import java.util.Objects;

/**
 * A single replicated write: the value {@code value} is to be written at
 * position {@code index} of the shared array, identified by {@link UpdateID}.
 */
public final class Update implements Serializable {

    public final UpdateID id;
    public final int index;
    public final int value;

    public Update(UpdateID id, int index, int value) {
        this.id = Objects.requireNonNull(id, "UpdateID must not be null");
        this.index = index;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Update)) return false;
        Update u = (Update) o;
        return index == u.index && value == u.value && id.equals(u.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index, value);
    }

    @Override
    public String toString() {
        return id + " positions[" + index + "]=" + value;
    }
}
