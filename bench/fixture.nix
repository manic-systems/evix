# Produces `breadth^(depth+1)` trivial, distinct derivations arranged in a
# `recurseForDerivations` tree so evix and nix-eval-jobs traverse it
# identically without any --force-recurse flag. The derivations are never
# built; only their `.drv` is computed, so evaluation is the only cost.
#
# This sucks. It *really* sucks.
{
  system ? builtins.currentSystem,
  breadth ? 4,
  depth ? 3,
}: let
  range = n: builtins.genList (i: i) n;

  mkDrv = name:
    derivation {
      inherit name system;
      builder = "/bin/sh";
      args = ["-c" "echo ok > $out"];
    };

  build = prefix: d:
    if d <= 0
    then
      # An attrset of derivations. It still needs the marker, or either evix nor
      # nix-eval-jobs will descend into it.
      {recurseForDerivations = true;}
      // builtins.listToAttrs (map (i: {
          name = "drv${toString i}";
          value = mkDrv "evix-fixture-${prefix}-${toString i}";
        })
        (range breadth))
    else
      {recurseForDerivations = true;}
      // builtins.listToAttrs (map (i: {
          name = "n${toString i}";
          value = build "${prefix}-${toString i}" (d - 1);
        })
        (range breadth));
in
  {recurseForDerivations = true;} // build "j" depth
