# Explanation

Understanding-oriented background. These pages explain *why* Hyperion is shaped
the way it is — read them when you want the mental model, not a recipe.

- **[Architecture](architecture.md)** — the layered (DDD) design and how a
  request flows through it.
- **[Assets and the Catalog](assets-and-catalog.md)** — the three asset types,
  schemas, and what the `Catalog` is responsible for.
- **[Ports and adapters](ports-and-adapters.md)** — the dependency-inversion
  layout, the composition root, and the enforced layering contract.
- **[Lite core and extras](extras-and-lite-core.md)** — why the default install
  is slim and how the extras map to capabilities.

For the concrete upgrade steps from a pre-1.0 release, see
[Migrating from pre-1.0](../reference/migration-pre-1.0.md).
